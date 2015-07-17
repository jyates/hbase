package org.kiji.maven.plugins.hbase;

import org.apache.maven.plugin.MojoExecutionException;

/**
 * Run the mini cluster and stay up until asked to shutdown
 * @goal run
 * @requiresDependencyResolution test
 */
public class RunMojo extends StartMojo {
  @Override
  public void execute() throws MojoExecutionException {
    super.execute();
    try {
      getLog().info("Namenode UI present at: http://" +  MiniHBaseClusterSingleton.INSTANCE
        .getClusterConfiguration().get("dfs.namenode.http-address")+"/webapps/hdfs/");
      getLog().info("Running cluster until interrupted....");
      MiniHBaseClusterSingleton.INSTANCE.waitUntilDone();
    } catch (InterruptedException e) {
      stop();
      throw new MojoExecutionException("Interrupted while waiting for cluster to complete", e);
    }
  }

  private void stop() {
    saveHadoopTmpDirectory();
    MiniHBaseClusterSingleton.INSTANCE.stop(getLog());
  }
}