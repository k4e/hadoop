package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

class CRException extends Exception {
  
  public CRException(String message) {
    super(message);
  }
  
  public CRException(Throwable t) {
    super(t);
  }
}
