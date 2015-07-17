package org.kiji.maven.plugins.hbase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.maven.plugin.AbstractMojo;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public abstract class BaseClusterMojo extends AbstractMojo {
  /**
   * If true, this goal should be a no-op.
   *
   * @parameter property="skip" default-value="false"
   */
  protected boolean mSkip;

  /**
   * If true, the Hadoop temporary directory (given by Hadoop configuration property hadoop.tmp
   * .dir) will be cleared before the cluster is started, then copied to the project's build
   * directory before the cluster is shutdown.
   *
   * @parameter property="save.hadoop.tmp" default-value="false"
   * @required
   */
  protected boolean mSaveHadoopTmpDir;

  /**
   * The build directory for this project.
   *
   * @parameter property="projectBuildDir" default-value="${project.build.directory}"
   * @required
   */
  private String mProjectBuildDir;

  /**
   * Sets the output directory for the project's build.
   *
   * @param dir The path to the output directory.
   */
  public void setProjectBuildDir(String dir) {
    mProjectBuildDir = dir;
  }


  /**
   * Sets whether this goal should be a no-op.
   *
   * @param skip If true, this goal should do nothing.
   */
  public void setSkip(boolean skip) {
    mSkip = skip;
  }

  /**
   * Sets whether the Hadoop temporary directory, given by hadoop.tmp.dir, should be cleared
   * before the cluster is started and copied to the project build directory before the cluster
   * is shutdown.
   *
   * @param saveTempDir If true, the directory will be copied to the project build directory
   *     before the cluster is shutdown.
   */
  public void setSaveHadoopTmpDir(boolean saveTempDir) {
    mSaveHadoopTmpDir = saveTempDir;
  }

  protected void removeHadoopTmpDir(Configuration conf){
    // If necessary, clear the Hadoop tmp dir.
    if (mSaveHadoopTmpDir) {
      removeHadoopTmpDirInternal(conf);
    }
  }

  /**
   * Deletes the directory given by hadoop.tmp.dir in the specified configuration. The
   * MapReduce cluster started by this plugin will store logs for job tasks in a job-specific
   * directory under hadoop.tmp.dir/userlogs. The
   * {@link org.apache.hadoop.hbase.HBaseTestingUtility} will delete log files on shutdown but
   * not the directory structure, making it hard to locate specific job logs after multiple runs.
   * Clearing hadoop.tmp.dir before the cluster starts again alleviates this problem.
   *
   * @param conf A Hadoop configuration used to determine the value of hadoop.tmp.dir.
   */
  private void removeHadoopTmpDirInternal(Configuration conf) {
    String hadoopTmpPath = conf.get("hadoop.tmp.dir");
    File hadoopTmp = new File(hadoopTmpPath);
    if (hadoopTmp.exists()) {
      getLog().info("Deleting Hadoop tmp dir " + hadoopTmp.toString() + " because it already " +
                    "exists.");
      try {
        FileUtils.deleteDirectory(hadoopTmp);
        getLog().info("Successfully deleted Hadoop tmp dir: " + hadoopTmp.toString());
      } catch (IOException e) {
        getLog().warn("An existing Hadoop tmp dir could not be deleted.", e);
      }
    }
  }

  protected void saveHadoopTmpDirectory(){
    if (mSaveHadoopTmpDir) {
      doCopyHadoopTmpDir();
    }
  }

  /**
   * Copies the directory indicated by hadoop.tmp.dir in the mini-cluster's configuration to the
   * project build directory.
   */
  private void doCopyHadoopTmpDir() {
    String tmpDirProperty =
      MiniHBaseClusterSingleton.INSTANCE.getClusterConfiguration().get("hadoop.tmp.dir");
    File hadoopTmp = new File(tmpDirProperty);
    File hadoopTmpCopy = new File(new File(mProjectBuildDir), "hadoop-tmp");
    getLog().info("Copying " + hadoopTmp.toString() + " to " + hadoopTmpCopy.toString());
    try {
      FileUtils.copyDirectory(hadoopTmp, hadoopTmpCopy);
      getLog().info("Successfully copied hadoop tmp dir.");
    } catch (IOException e) {
      getLog().warn("The Hadoop tmp dir could not be copied to the project's build directory.", e);
    }
  }
}
