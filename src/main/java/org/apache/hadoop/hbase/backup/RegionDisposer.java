package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * Utility class to handle the removal of the files for a HRegion from the
 * {@link FileSystem}. The {@link HFile HFiles} will be archived or deleted,
 * depending on the state of the system.
 */
public class RegionDisposer {
  private static final Log LOG = LogFactory.getLog(RegionDisposer.class);
  private static final String SEPARATOR = ".";

  private RegionDisposer() {
    // hidden ctor since this is just a util
  }

  /**
   * Cleans up all the files for a HRegion, either via archiving (if the
   * {@link HFileArchiveMonitor} indicates it should be) or by just removing all
   * the files.
   * @param fs the file system object
   * @param monitor manager for if the region should be archived or deleted
   * @param info HRegionInfo for region to be deleted
   * @throws IOException
   */
  public static void deposeRegion(FileSystem fs, HFileArchiveMonitor monitor, HRegionInfo info)
      throws IOException {
    Path rootDir = FSUtils.getRootDir(fs.getConf());
    RegionDisposer.deposeRegion(fs, monitor, rootDir,
      HTableDescriptor.getTableDir(rootDir, info.getTableName()),
      HRegion.getRegionDir(rootDir, info));
  }

  /**
   * Cleans up all the files for a HRegion, either via archiving (if the
   * {@link HFileArchiveMonitor}, from the {@link RegionServerServices},
   * indicates it should be) or by just removing all the files.
   * @param fs FileSystem where the region files reside
   * @param rss services to obtain the {@link HFileArchiveMonitor} via
   *          {@link RegionServerServices#getHFileArchiveMonitor()}. Testing
   *          notes: if the returned monitor is null, the region files are
   *          deleted
   * @param rootdir root directory of the hbase installation on the
   *          {@link FileSystem}
   * @param tabledir {@link Path} to where the table is being stored (for
   *          building the archive path)
   * @param regiondir {@link Path} to where a region is being stored (for
   *          building the archive path)
   * @throws IOException
   */
  public static void deposeRegion(FileSystem fs, RegionServerServices rss, Path rootdir,
      Path tabledir, Path regiondir) throws IOException {
    HFileArchiveMonitor manager = rss == null ? null : rss.getHFileArchiveMonitor();
    deposeRegion(fs, manager, rootdir, tabledir, regiondir);
  }

  /**
   * Remove an entire region from the table directory.
   * <p>
   * Either archives the region or outright deletes it, depending on if
   * archiving is enabled.
   * @param fs {@link FileSystem} from which to remove the region
   * @param monitor Monitor for which tables should be archived or deleted
   * @param rootdir {@link Path} to the root directory where hbase files are
   *          stored (for building the archive path)
   * @param tabledir {@link Path} to where the table is being stored (for
   *          building the archive path)
   * @param regionDir {@link Path} to where a region is being stored (for
   *          building the archive path)
   * @return <tt>true</tt> if the region was sucessfully deleted. <tt>false</tt>
   *         if the filesystem operations could not complete.
   * @throws IOException if the request cannot be completed
   */
  public static boolean deposeRegion(FileSystem fs, HFileArchiveMonitor monitor, Path rootdir,
      Path tabledir, Path regionDir) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("REMOVING region " + regionDir.toString());
    }
    // check to make sure we don't keep files, in which case just delete them
    // NOTE: assumption here that tabledir = tablename
    if (shouldDeleteFiles(monitor, tabledir)) {
      LOG.debug("Deleting hregion directory (" + regionDir + ") - no backup");
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    // otherwise, we archive the files
    // make sure the regiondir lives under the tabledir
    Preconditions.checkArgument(regionDir.toString().startsWith(tabledir.toString()));

    // get the directory to archive region files
    Path regionArchiveDir = HFileArchiveUtil.getRegionArchiveDir(fs.getConf(), tabledir, regionDir);
    if (regionArchiveDir == null) {
      LOG.warn("No archive directory could be found for the region:" + regionDir
          + ", deleting instead");
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    FileStatusConverter getAsFile = new FileStatusConverter(fs);
    // otherwise, we attempt to archive the store files

    // build collection of just the store directories to archive
    Collection<File> toArchive = Collections2.transform(
      Collections2.filter(Arrays.asList(fs.listStatus(regionDir)), new Predicate<FileStatus>() {
        @Override
        public boolean apply(FileStatus input) {
          // filter out paths that are not store directories
          if (!input.isDir() || input.getPath().getName().toString().startsWith(".")) return false;
          return true;
        }
      }), getAsFile);

    boolean success = false;
    try {
      success = resolveAndArchive(fs, regionArchiveDir, toArchive);
    } catch (IOException e) {
      success = false;
    }

    // if that was successful, then we delete the region
    if (success) {
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    LOG.warn("Received error when attempting to archive files, not deleteing region directory.");

    return success;
  }

  /**
   * Without regard for backup, delete a region. Should be used with caution.
   * @param regionDir {@link Path} to the region to be deleted.
   * @throws IOException on filesystem operation failure
   */
  private static boolean deleteRegionWithoutArchiving(FileSystem fs, Path regionDir)
      throws IOException {
    if (fs.delete(regionDir, true)) {
      LOG.debug("Deleted all region files in: " + regionDir);
      return true;
    }
    LOG.debug("Failed to delete region directory:" + regionDir);
    return false;
  }

  /**
   * Remove the store files, either by archiving them or outright deletion
   * @param rss services to obtain the {@link HFileArchiveMonitor} via
   *          {@link RegionServerServices#getHFileArchiveMonitor()}. Testing
   *          notes: if the services or the returned monitor is null, the region
   *          files are deleted
   * @param parent Parent region hosting the store files
   * @param conf {@link Configuration} to examine to determine the archive
   *          directory
   * @param family the family hosting the store files
   * @param compactedFiles files to be disposed of. No further reading of these
   *          files should be attempted; otherwise likely to cause an
   *          {@link IOException}
   * @throws IOException if the files could not be correctly disposed.
   */
  public static void disposeStoreFiles(RegionServerServices rss, HRegion parent,
      Configuration conf, byte[] family, Collection<StoreFile> compactedFiles) throws IOException {

    // short circuit if we don't have any files to delete
    if (compactedFiles.size() == 0) {
      LOG.debug("No store files to dispose, done!");
      return;
    }

    // if the monitor isn't available or indicates we shouldn't archive, then we
    // delete the store files
    if (shouldDeleteFiles(rss, parent.getTableDir())) {
      deleteStoreFilesWithoutArchiving(compactedFiles);
      return;
    }

    // otherwise we attempt to archive the store files
    LOG.debug("Archiving compacted store files.");
    FileSystem fs = rss.getFileSystem();
    StoreToFile getStorePath = new StoreToFile(fs);
    Collection<File> storeFiles = Collections2.transform(compactedFiles, getStorePath);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, parent, family);

    // make sure we don't archive if we can't and that the archive dir exists
    if (storeArchiveDir == null || !fs.mkdirs(storeArchiveDir)) {
      LOG.warn("Could make archive directory (" + storeArchiveDir + ") for store:"
          + Bytes.toString(family) + ", deleting compacted files instead.");
    }

    // do the actual archive
    resolveAndArchive(fs, storeArchiveDir, storeFiles);
  }

  /**
   * Check to see if we should archive the given files, based on the state of
   * the regionserver
   * @param rss services to provide the {@link HFileArchiveMonitor}, can be null
   *          (in which case returns <tt>true</tt>).
   * @param tabledir table directory where the files are being archived. NOTE:
   *          implicit assumption here that the tabledir = name of the table
   * @return <tt>true</tt> if the files should be deleted <tt>false</tt> if they
   *         should be archived
   */
  private static boolean shouldDeleteFiles(RegionServerServices rss, Path tabledir) {
    if (rss == null) return true;
    return shouldDeleteFiles(rss.getHFileArchiveMonitor(), tabledir);
  }

  /**
   * Check to see if we should archive the given files, based on the state of
   * the regionserver
   * @param monitor services to indicate if the files should be archived and can
   *          be null (in which case returns <tt>true</tt>).
   * @param tabledir table directory where the files are being archived. NOTE:
   *          implicit assumption here that the tabledir = name of the table
   * @return <tt>true</tt> if the files should be deleted, <tt>false</tt> if
   *         they should be archived
   */
  private static boolean shouldDeleteFiles(HFileArchiveMonitor monitor, Path tabledir) {
    if (monitor == null) return true;
    return !monitor.archiveHFiles(tabledir.getName());
  }

  /**
   * Just do a simple delete of the given store files
   * <p>
   * A best effort is made to delete each of the files, rather than bailing on
   * the first failure.
   * <p>
   * This method is preferable to
   * {@link #deleteFilesWithoutArchiving(Collection)} since it consumes less
   * resources, but is limited in terms of usefulness
   * @param compactedFiles store files to delete from the file system.
   * @throws IOException if a file cannot be deleted. All files will be
   *           attempted to deleted before throwing the exception, rather than
   *           failing at the first file.
   */
  private static void deleteStoreFilesWithoutArchiving(Collection<StoreFile> compactedFiles)
      throws IOException {
    LOG.debug("Deleting store files without archiving.");
    boolean failure = false;
    for (StoreFile hsf : compactedFiles) {
      try {
        hsf.deleteReader();
      } catch (IOException e) {
        LOG.error("Failed to delete store file:" + hsf.getPath());
        failure = true;
      }
    }
    if (failure) {
      throw new IOException("Failed to delete all store files. See log for failures.");
    }
  }

  /**
   * Archive the given files and resolve any conflicts with existing files via
   * appending the time archiving started (so all conflicts in the same group
   * have the same timestamp appended).
   * <p>
   * Recursively copies over files to archive if any of the files to archive are
   * directories. Archive directory structure for children is the base archive
   * directory name + the parent directory.
   * @param fs {@link FileSystem} on which to archive the files
   * @param baseArchiveDir base archive directory to archive the given files
   * @param toArchive files to be archived
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   * @throws IOException on unexpected failure
   */
  private static boolean resolveAndArchive(FileSystem fs, Path baseArchiveDir,
      Collection<File> toArchive)
      throws IOException {
    long start = EnvironmentEdgeManager.currentTimeMillis();
    List<File> failures = resolveAndArchive(fs, baseArchiveDir, toArchive, start);

    // clean out the failures by just deleting them
    if (failures.size() > 0) {
      try {
        LOG.warn("Failed to complete archive, so just deleting extra store files.");
        deleteFilesWithoutArchiving(failures);
      } catch (IOException e) {
        LOG.debug("Failed to delete store file(s) when archiving failed", e);
      }
      return false;
    }
    return true;
  }

  /**
   * Resolve any conflict with an existing archive file via timestamp-append
   * renaming of the existing file and then archive the passed in files.
   * @param fs {@link FileSystem} on which to archive the files
   * @param baseArchiveDir base archive directory to store the files. If any of
   *          the files to archive are directories, will append the name of the
   *          directory to the base archive directory name, creating a parallel
   *          structure.
   * @param toArchive files/directories that need to be archvied
   * @param start time the archiving started - used for resolving archive
   *          conflicts.
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   * @throws IOException if an unexpected file operation exception occured
   */
  private static List<File> resolveAndArchive(FileSystem fs, Path baseArchiveDir,
      Collection<File> toArchive, long start) throws IOException {
    // short circuit if no files to move
    if (toArchive.size() == 0) return Collections.emptyList();

    LOG.debug("moving files to the archive directory: " + baseArchiveDir);
    // make sure the archive directory exists
    if (!fs.exists(baseArchiveDir)) {
      if (!fs.mkdirs(baseArchiveDir)) {
        throw new IOException(
          "Failed to create the archive directory:" + baseArchiveDir
              + ", quitting archive attempt.");
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Created archive directory:" + baseArchiveDir);
      }
    }

    List<File> failures = new ArrayList<File>();
    String startTime = Long.toString(start);
    for (File file : toArchive) {
      // if its a file archive it
      try {
        LOG.debug("Archiving:" + file);
        if (file.isFile()) {
          // attempt to archive the file
          if (!resolveAndArchiveFile(baseArchiveDir, file, startTime)) {
            LOG.warn("Couldn't archive " + file + " into backup directory: " + baseArchiveDir);
            failures.add(file);
          }
        } else {
          // otherwise its a directory and we need to archive the whole
          // directory
          // so we add the directory name to the one base archive
          LOG.debug(file + " is a directory, archiving children files");
          Path directoryArchiveDir = new Path(baseArchiveDir, file.getName());
          // get all the files from that directory and attempt to archive
          // those too
          Collection<File> children = file.getChildren();
          failures.addAll(resolveAndArchive(fs, directoryArchiveDir, children, start));
        }
      } catch (IOException e) {
        LOG.warn("Failed to archive file: " + file, e);
        failures.add(file);
      }
    }
    return failures;
  }

  /**
   * Attempt to archive the passed in file to the store archive directory.
   * <p>
   * If the same file already exists in the archive, it is moved to a
   * timestamped directory under the archive directory and the new file is put
   * in its place.
   * @param fs FileSystem on which to move files
   * @param storeArchiveDirectory {@link Path} to the directory that stores the
   *          archives of the hfiles
   * @param currentFile {@link Path} to the original HFile that will be archived
   * @param archiveStartTime
   * @return <tt>true</tt> if the file is successfully archived. <tt>false</tt>
   *         if there was a problem, but the operation still completed.
   * @throws IOException on failure to complete {@link FileSystem} operations.
   */
  private static boolean resolveAndArchiveFile(Path storeArchiveDirectory, File currentFile,
      String archiveStartTime) throws IOException {
    // build path as it should be in the archive
    String filename = currentFile.getName();
    Path archiveFile = new Path(storeArchiveDirectory, filename);
    FileSystem fs = currentFile.getFileSystem();
    // if the file already exists in the archive, move that one to a
    // timestamped backup
    if (fs.exists(archiveFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("File:" + archiveFile
            + " already exists in archive, moving to timestamped backup and overwriting current.");
      }

      Path backedupArchiveFile = new Path(storeArchiveDirectory, filename + SEPARATOR
          + archiveStartTime);

      // move the archive file to the stamped backup
      if (!fs.rename(archiveFile, backedupArchiveFile)) {
        LOG.warn("Could not rename archive file to backup: " + backedupArchiveFile
            + ", deleting existing file in favor of newer.");
        fs.delete(archiveFile, false);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Backed up archive file from: " + archiveFile);
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No existing file in archive for:" + archiveFile
          + ", free to archive original file.");
    }

    // at this point, we should have a free spot for the archive file
    if (currentFile.moveAndClose(archiveFile)) {
      LOG.error("Failed to archive file:" + currentFile);
      return false;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Finished archiving file from: " + currentFile + ", to: " + archiveFile);
    }
    return true;
  }

  /**
   * Simple delete of regular files from the {@link FileSystem}.
   * <p>
   * This method is a more generic implementation that the other deleteXXX
   * methods in this class, allowing more code reuse at the cost of a couple
   * more, short-lived objects (which should have minimum impact on the jvm).
   * @param fs {@link FileSystem} where the files live
   * @param files {@link Collection} of files to be deleted
   * @throws IOException if a file cannot be deleted. All files will be
   *           attempted to deleted before throwing the exception, rather than
   *           failing at the first file.
   */
  private static void deleteFilesWithoutArchiving(Collection<File> files) throws IOException {
    boolean failure = false;
    for (File file : files) {
      try {
        LOG.debug("Deleting region file:" + file);
        file.delete();
      } catch (IOException e) {
        LOG.error("Failed to delete file:" + file);
        failure = true;
      }
    }
    if (failure) {
      throw new IOException("Failed to delete all store files. See log for failures.");
    }
  }

  /**
   * Adapt a type to match the {@link File} interface, which is used internally
   * for handling archival/removal of files
   * @param <T> type to adapt to the {@link File} interface
   */
  private static abstract class FileConverter<T> implements Function<T, File> {
    protected final FileSystem fs;
    public FileConverter(FileSystem fs) {
      this.fs = fs;
    }
  }

  /**
   * Convert a FileStatus to something we can manage in the archiving
   */
  private static class FileStatusConverter extends FileConverter<FileStatus> {
    public FileStatusConverter(FileSystem fs) {
      super(fs);
    }

    @Override
    public File apply(FileStatus input) {
      return new FileablePath(fs, input.getPath());
    }
  }

  /**
   * Convert the {@link StoreFile} into something we can manage in the archive
   * methods
   */
  private static class StoreToFile extends FileConverter<StoreFile> {
    public StoreToFile(FileSystem fs) {
      super(fs);
    }

    @Override
    public File apply(StoreFile input) {
      return new FileableStoreFile(fs, input);
    }
  }

  /**
   * Wrapper to handle file operations uniformly
   */
  private static abstract class File {
    protected final FileSystem fs;

    public File(FileSystem fs) {
      this.fs = fs;
    }

    /**
     * Delete the file
     * @throws IOException on failure
     */
    abstract void delete() throws IOException;

    /**
     * Check to see if this is a file or a directory
     * @return <tt>true</tt> if it is a file, <tt>false</tt> otherwise
     * @throws IOException on {@link FileSystem} connection error
     */
    abstract boolean isFile() throws IOException;

    /**
     * @return if this is a directory, returns all the children in the
     *         directory, otherwise returns an empty list
     * @throws IOException
     */
    abstract Collection<File> getChildren() throws IOException;

    /**
     * close any outside readers of the file
     * @throws IOException
     */
    abstract void close() throws IOException;

    /**
     * @return the name of the file (not the full fs path, just the individual
     *         file name)
     */
    abstract String getName();

    /**
     * @return the path to this file
     */
    abstract Path getPath();

    /**
     * Move the file to the given destination
     * @param dest
     * @return <tt>true</tt> on success
     * @throws IOException
     */
    public boolean moveAndClose(Path dest) throws IOException {
      this.close();
      Path p = this.getPath();
      return !fs.rename(p, dest);
    }

    /**
     * @return the {@link FileSystem} on which this file resides
     */
    public FileSystem getFileSystem() {
      return this.fs;
    }

    @Override
    public String toString() {
      return this.getClass() + ", file:" + getPath().toString();
    }
  }

  private static class FileablePath extends File {
    private final Path file;
    private final FileStatusConverter getAsFile;

    public FileablePath(FileSystem fs, Path file) {
      super(fs);
      this.file = file;
      this.getAsFile = new FileStatusConverter(fs);
    }

    @Override
    public void delete() throws IOException {
      fs.delete(file, true);
    }

    @Override
    public String getName() {
      return file.getName();
    }

    @Override
    public Collection<File> getChildren() throws IOException {
      if (fs.isFile(file)) return Collections.emptyList();
      return Collections2.transform(Arrays.asList(fs.listStatus(file)), getAsFile);
    }

    @Override
    public boolean isFile() throws IOException {
      return fs.isFile(file);
    }

    @Override
    public void close() throws IOException {
      // NOOP -files are implicitly close
    }

    @Override
    Path getPath() {
      return file;
    }
  }

  private static class FileableStoreFile extends File {
    StoreFile file;

    public FileableStoreFile(FileSystem fs, StoreFile store) {
      super(fs);
      this.file = store;
    }

    @Override
    public void delete() throws IOException {
      file.deleteReader();
    }

    @Override
    public String getName() {
      return file.getPath().getName();
    }

    @Override
    public boolean isFile() {
      return true;
    }

    @Override
    public Collection<File> getChildren() throws IOException {
      return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
      file.closeReader(true);
    }

    @Override
    Path getPath() {
      return file.getPath();
    }
  }
}
