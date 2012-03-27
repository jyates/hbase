package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class HFileArchiveTestingUtil {

  private HFileArchiveTestingUtil() {
    // NOOP private ctor since this is just a utility class
  }

  /**
   * Compare the archived files to the files in the original directory
   * @param previous original files that should have been archived
   * @param archived files that were archived
   * @param fs filessystem on which the archiving took place
   * @throws IOException
   */
  public static void compareArchiveToOriginal(FileStatus[] previous,
      FileStatus[] archived, FileSystem fs) throws IOException {
    compareArchiveToOriginal(previous, archived, fs, false);
  }

  /**
   * Compare the archived files to the files in the original directory
   * @param previous original files that should have been archived
   * @param archived files that were archived
   * @param fs filessystem on which the archiving took place
   * @param hasTimedBackup <tt>true</tt> if we expect to find an archive backup
   *          directory with a copy of the files in the archive directory (and
   *          the original files).
   * @throws IOException
   */
  public static void compareArchiveToOriginal(FileStatus[] previous,
      FileStatus[] archived, FileSystem fs, boolean hasTimedBackup)
      throws IOException {
    boolean hasBackup = false;
    List<String> originalFileNames = new ArrayList<String>(previous.length);
    for (FileStatus f : previous) {
      originalFileNames.add(f.getPath().getName());
    }
    Collections.sort(originalFileNames);

    List<String> currentFiles = new ArrayList<String>(previous.length);
    for (FileStatus f : archived) {
      // if its a directory, then examine the files in that directory
      if (f.isDir()) {
        compareArchiveToOriginal(previous, fs.listStatus(f.getPath()), fs,
          false);
        hasBackup = true;
      } else {
        // otherwise, make compare it to the original store files
        currentFiles.add(f.getPath().getName());
      }
    }
    Collections.sort(currentFiles);

    assertEquals(
      "Not the same numbe of current files\n" + compareFileLists(originalFileNames, currentFiles),
      originalFileNames.size(),
      currentFiles.size());
    assertEquals(
      "Different backup files, but same amount\n"
          + compareFileLists(originalFileNames, currentFiles), originalFileNames, currentFiles);
    assertEquals("Expected a backup dir, but none was found", hasTimedBackup, hasBackup);
  }

  /* Get a pretty representation of the differences */
  private static String compareFileLists(List<String> expected, List<String> gotten) {
    StringBuilder sb = new StringBuilder("Expected (" + expected.size() + "): \t\t Gotten ("
        + gotten.size() + "):\n");
    List<String> notFound = new ArrayList<String>();
    for (String s : expected) {
      if (gotten.contains(s)) sb.append(s + "\t\t" + s + "\n");
      else notFound.add(s);
    }
    sb.append("Not Found:\n");
    for (String s : notFound) {
      sb.append(s + "\n");
    }
    sb.append("\nExtra:\n");
    for (String s : gotten) {
      if (!expected.contains(s)) sb.append(s + "\n");
    }
    return sb.toString();
  }
}
