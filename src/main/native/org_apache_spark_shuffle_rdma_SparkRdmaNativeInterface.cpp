#include "org_apache_spark_shuffle_rdma_SparkRdmaNativeInterface.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static jfieldID fd_fdID;
static  jclass clazz;

/**
 * Throw a Java exception by name. Similar to SignalError.
 */
JNIEXPORT void JNICALL JNU_ThrowByName(JNIEnv *env, const char *name,
                                       const char *msg)
{
  jclass cls = env->FindClass(name);

  if (cls != 0) /* Otherwise an exception has already been thrown */
    env->ThrowNew(cls, msg);
}

int fdval(JNIEnv *env, jobject fdo)
{
    clazz = env->FindClass("java/io/FileDescriptor");
    fd_fdID = env->GetFieldID(clazz, "fd", "I");
    return env->GetIntField(fdo, fd_fdID);
}

/* Throw an IOException, using provided message string.
 */
void JNU_ThrowIOException(JNIEnv *env, const char *msg)
{
  JNU_ThrowByName(env, "java/io/IOException", msg);
}

void JNU_ThrowOutOfMemoryError(JNIEnv *env, const char *msg)
{
  JNU_ThrowByName(env, "java/lang/OutOfMemoryError", msg);
}

void JNU_ThrowIOExceptionWithLastError(JNIEnv *env, const char *msg)
{
  char *errorMsg;
  asprintf(&errorMsg, "%s: %s\n", msg, strerror(errno));
  JNU_ThrowIOException(env, errorMsg);
  free(errorMsg);
}

JNIEXPORT void JNICALL Java_org_apache_spark_shuffle_rdma_SparkRdmaNativeInterface_unmap(JNIEnv *, jobject, jlong addr, jlong length)
{
  munmap((void *)addr, length);
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_shuffle_rdma_SparkRdmaNativeInterface_map(JNIEnv *env, jobject obj, jobject fdo, jlong off, jlong len)
{
  jint fd = fdval(env, fdo);
  int prot = PROT_WRITE | PROT_READ;
  int flags = MAP_SHARED;
  void *mapAddress = 0;

  mapAddress = mmap(
      0,     /* Let OS decide location */
      len,   /* Number of bytes to map */
      prot,  /* File permissions */
      flags, /* Changes are shared */
      fd,    /* File descriptor of mapped file */
      off);  /* Offset into file */

  if (mapAddress == MAP_FAILED)
  {
    if (errno == ENOMEM)
    {
      JNU_ThrowOutOfMemoryError(env, "Map failed");
      return -1;
    }
    JNU_ThrowIOExceptionWithLastError(env, "Map failed");
  }

  return ((jlong)(unsigned long)mapAddress);
}
