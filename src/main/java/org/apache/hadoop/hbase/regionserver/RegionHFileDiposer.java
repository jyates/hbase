package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.HFileArchiveMonitor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

import com.google.common.base.Preconditions;

/**
 * Utility class to handle the deletion/archival of HFiles for a {@link HRegion}
 */
public class RegionHFileDiposer {
  private static final Log LOG = LogFactory.getLog(RegionHFileDiposer.class);

  private RegionHFileDiposer() {
    // hidden ctor since this is just a util
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
  public static boolean dispose(FileSystem fs, HFileArchiveMonitor monitor, Path rootdir,
      Path tabledir, Path regionDir) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("REMOVING region " + regionDir.toString());
    }
    // check to make sure we don't keep files, in which case just delete them
    String table = tabledir.getName();
    if (monitor == null || !monitor.archiveHFiles(table)) {
      LOG.debug("Doing raw delete of hregion directory (" + regionDir + ") - no backup");
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("ARCHIVING HFiles for region in table: " + table + " to " + regionArchiveDir);
    }

    // get the path to each of the store directories
    FileStatus[] stores = fs.listStatus(regionDir);
    // if there are no stores, just remove the region
    if (stores == null || stores.length == 0) {
      LOG.debug("No stores present in region:" + regionDir.getName() + " for table" + table
          + ", done archiving.");
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    // otherwise, we attempt to archive the store files
    boolean failure = false;
    for (FileStatus storeDir : stores) {
      Path storeArchiveDir = new Path(regionArchiveDir, storeDir.getPath().getName());
      if (!resolveAndArchive(fs, storeArchiveDir, storeDir.getPath())) {
        LOG.warn("Failed to archive all files in store directory: " + storeDir.getPath());
        failure = true;
      }
    }
    return failure;
  }

  /**
   * Resolve all the copies of files. Ensures that no two files will collide by
   * moving existing archived files to a timestampted directory and the curent
   * archive files to their place. Otherwise, just moves the files into the
   * archive directory
   * @param fs filesystem on which all the files live
   * @param storeArchiveDirectory path to the archive of the store directory
   *          (already exists)
   * @param store path to the store directory
   * @return <tt>true</tt> if all files are moved successfully, <tt>false</tt>
   *         otherwise.
   */
  private static boolean resolveAndArchive(FileSystem fs, Path storeArchiveDirectory, Path store)
      throws IOException {
    FileStatus[] storeFiles = fs.listStatus(store);
    // if there are no store files to move, we are done
    if (storeFiles == null || storeFiles.length == 0) return true;

    String archiveStartTime = Long.toString(EnvironmentEdgeManager.currentTimeMillis());
    boolean result = true;
    for (FileStatus stat : storeFiles) {
      Path file = stat.getPath();
      // resolve copy over each of the files to be archived.
      try {
        if (!HFileArchiveUtil.resolveAndArchiveFile(fs, storeArchiveDirectory, file,
          archiveStartTime)) {
          result = false;
          LOG.warn("Failed to archive file: " + file);
        }
      } catch (IOException e) {
        result = false;
        LOG.warn("Failed to archive file: " + file, e);
      }
    }
    return result;
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
}
