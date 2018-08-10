// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_UTIL_JNI_UTIL_H
#define IMPALA_UTIL_JNI_UTIL_H

#include <jni.h>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gutil/macros.h"

#define THROW_IF_ERROR_WITH_LOGGING(stmt, env, adaptor) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      (adaptor)->WriteErrorLog(); \
      (adaptor)->WriteFileErrors(); \
      (env)->ThrowNew((adaptor)->impala_exc_cl(), status.GetDetail().c_str()); \
      return; \
    } \
  } while (false)

#define THROW_IF_ERROR(stmt, env, impala_exc_cl) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      (env)->ThrowNew((impala_exc_cl), status.GetDetail().c_str()); \
      return; \
    } \
  } while (false)

#define THROW_IF_ERROR_RET(stmt, env, impala_exc_cl, ret) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      (env)->ThrowNew((impala_exc_cl), status.GetDetail().c_str()); \
      return (ret); \
    } \
  } while (false)

#define THROW_IF_EXC(env, exc_class) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      DCHECK((throwable_to_string_id_) != NULL); \
      jstring stack = (jstring) env->CallStaticObjectMethod(JniUtil::jni_util_class(), \
          (JniUtil::throwable_to_stack_trace_id()), exc); \
      jboolean is_copy; \
      const char* c_stack = \
          reinterpret_cast<const char*>((env)->GetStringUTFChars(stack, &is_copy)); \
      (env)->ExceptionClear(); \
      (env)->ThrowNew((exc_class), c_stack); \
      return; \
    } \
  } while (false)

#define RETURN_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      jstring stack = (jstring) env->CallStaticObjectMethod(JniUtil::jni_util_class(), \
          (JniUtil::throwable_to_stack_trace_id()), exc); \
      jboolean is_copy; \
      const char* c_stack = \
          reinterpret_cast<const char*>((env)->GetStringUTFChars(stack, &is_copy)); \
      VLOG(1) << string(c_stack); \
      return; \
    } \
  } while (false)

#define EXIT_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      jstring stack = (jstring) env->CallStaticObjectMethod(JniUtil::jni_util_class(), \
          (JniUtil::throwable_to_stack_trace_id()), exc); \
      jboolean is_copy; \
      const char* c_stack = \
          reinterpret_cast<const char*>((env)->GetStringUTFChars(stack, &is_copy)); \
      LOG(FATAL) << string(c_stack); \
    } \
  } while (false)

#define RETURN_ERROR_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) return JniUtil::GetJniExceptionMsg(env);\
  } while (false)

/// C linkage for helper functions in hdfsJniHelper.h
extern  "C" { extern JNIEnv* getJNIEnv(void); }

namespace impala {

class Status;

/// Utility class to push/pop a single JNI frame. "push" will push a JNI frame and the
/// d'tor will pop the JNI frame. Frames establish a scope for local references. Local
/// references go out of scope when their frame is popped, which enables the GC to clean up
/// the corresponding objects.
class JniLocalFrame {
 public:
  JniLocalFrame(): env_(NULL) {}
  ~JniLocalFrame() { if (env_ != NULL) env_->PopLocalFrame(NULL); }

  /// Pushes a new JNI local frame. The frame can support max_local_ref local references.
  /// The number of local references created inside the frame might exceed max_local_ref,
  /// but there is no guarantee that memory will be available.
  /// Push should be called at most once.
  Status push(JNIEnv* env, int max_local_ref = 10) WARN_UNUSED_RESULT;

 private:
  DISALLOW_COPY_AND_ASSIGN(JniLocalFrame);

  JNIEnv* env_;
};

/// Describes one method to look up in a Java object
struct JniMethodDescriptor {
  /// Name of the method, case must match
  const std::string name;

  /// JNI-style method signature
  const std::string signature;

  /// Handle to the method
  jmethodID* method_id;
};

/// Helper class for lifetime management of chars from JNI, releasing JNI chars when
/// destructed
class JniUtfCharGuard {
 public:
  /// Construct a JniUtfCharGuards holding nothing
  JniUtfCharGuard() : utf_chars(nullptr) {}

  /// Release the held char sequence if there is one.
  ~JniUtfCharGuard() {
    if (utf_chars != nullptr) env->ReleaseStringUTFChars(jstr, utf_chars);
  }

  /// Try to get chars from jstr. If error is returned, utf_chars and get() remain
  /// to be nullptr, otherwise they point to a valid char sequence. The char sequence
  /// lives as long as this guard. jstr should not be null.
  static Status create(JNIEnv* env, jstring jstr, JniUtfCharGuard* out);

  /// Get the char sequence. Returns nullptr if the guard does hold a char sequence.
  const char* get() { return utf_chars; }
 private:
  JNIEnv* env;
  jstring jstr;
  const char* utf_chars;
  DISALLOW_COPY_AND_ASSIGN(JniUtfCharGuard);
};

class JniScopedArrayCritical {
 public:
  /// Construct a JniScopedArrayCritical holding nothing.
  JniScopedArrayCritical():  env_(nullptr), jarr_(nullptr), arr_(nullptr), size_(0) {}

  /// Release the held byte[] contents if necessary.
  ~JniScopedArrayCritical() {
    if (env_ != nullptr && jarr_ != nullptr && arr_ != nullptr) {
      env_->ReleasePrimitiveArrayCritical(jarr_, arr_, JNI_ABORT);
    }
  }

  /// Try to get the contents of 'jarr' via JNIEnv::GetPrimitiveArrayCritical() and set
  /// the results in 'out'. Returns true upon success and false otherwise. If false is
  /// returned 'out' is not modified.
  static bool Create(JNIEnv* env, jbyteArray jarr, JniScopedArrayCritical* out)
      WARN_UNUSED_RESULT;

  uint8_t* get() const { return arr_; }

  int size() const { return size_; }
 private:
  JNIEnv* env_;
  jbyteArray jarr_;
  uint8_t* arr_;
  int size_;
  DISALLOW_COPY_AND_ASSIGN(JniScopedArrayCritical);
};

/// Utility class for making JNI calls, with various types of argument
/// or response.
///
/// Example usages:
///
/// 1) Static call taking a Thrift struct and returning a string:
///
///   string s;
///   RETURN_IF_ERROR(JniCall(my_method).on_class(my_jclass)
///       .with_thrift_arg(foo).Call(&s));
///
/// 2) Non-static call taking no arguments and returning a Thrift struct:
///
///   TMyObject result;
///   RETURN_IF_ERROR(JniCall(my_method).on_instance(my_jobject)
///       .Call(&result);
class JniCall {
 public:
  explicit JniCall(jmethodID method)
    : method_(DCHECK_NOTNULL(method)),
      env_(getJNIEnv()) {
    status_ = frame_.push(env_);
  }

  JniCall& on_class(jclass cls) {
    DCHECK(!instance_ && !class_);
    class_ = DCHECK_NOTNULL(cls);
    return *this;
  }

  JniCall& on_instance(jobject obj) {
    DCHECK(!instance_ && !instance_);
    instance_ = DCHECK_NOTNULL(obj);
    return *this;
  }

  template<class T>
  JniCall& with_thrift_arg(const T& arg) {
    if (!status_.ok()) return *this;
    DCHECK(!arg_) << "support only single arg";
    jbyteArray bytes;
    status_ = SerializeThriftMsg(env_, &arg, &bytes);
    if (status_.ok()) arg_ = bytes;
    return *this;
  }

  /// Call the method expecting no result.
  Status Call() WARN_UNUSED_RESULT {
    return Call(static_cast<void*>(nullptr));
  }

  /// Call the method and return a result (either std::string or a Thrift struct).
  template<class T>
  Status Call(T* result) WARN_UNUSED_RESULT;

 private:
  template<class T>
  Status ObjectToResult(jobject obj, T* result) {
    DCHECK(obj) << "Call returned unexpected null Thrift object";
    RETURN_IF_ERROR(DeserializeThriftMsg(env_, static_cast<jbyteArray>(obj), result));
    return Status::OK();
  }

  Status ObjectToResult(jobject obj, void* no_result) {
    return Status::OK();
  }

  Status ObjectToResult(jobject obj, std::string* result);

  const jmethodID method_;
  JNIEnv* const env_;
  JniLocalFrame frame_;

  jclass class_ = nullptr;
  jobject instance_ = nullptr;
  jobject arg_ = nullptr;
  Status status_;

  DISALLOW_COPY_AND_ASSIGN(JniCall);
};


/// Utility class for JNI-related functionality.
/// Init() should be called as soon as the native library is loaded.
/// Creates global class references, and promotes local references to global references.
/// Attention! Lifetime of JNI components and common pitfalls:
/// 1. JNIEnv* cannot be shared among threads, so it should NOT be globally cached.
/// 2. References created via jnienv->New*() calls are local references that go out of scope
///    at the end of a code block (and will be gc'ed by the JVM). They should NOT be cached.
/// 3. Use global references for caching classes.
///    They need to be explicitly created and cleaned up (will not be gc'd up by the JVM).
///    Global references can be shared among threads.
/// 4. JNI method ids and field ids are tied to the JVM that created them,
///    and can be shared among threads. They are not "references" so there is no need
///    to explicitly create a global reference to them.
class JniUtil {
 public:
  /// Call this prior to any libhdfs calls.
  static void InitLibhdfs();

  /// Find JniUtil class, and get JniUtil.throwableToString method id
  static Status Init() WARN_UNUSED_RESULT;

  /// Initializes the JvmPauseMonitor.
  static Status InitJvmPauseMonitor() WARN_UNUSED_RESULT;

  /// Returns true if the given class could be found on the CLASSPATH in env.
  /// Returns false otherwise, or if any other error occurred (e.g. a JNI exception).
  /// This function does not log any errors or exceptions.
  static bool ClassExists(JNIEnv* env, const char* class_str);

  /// Return true if the given class has a non-static method with a specific name and
  /// signature. Returns false otherwise, or if any other error occurred
  /// (e.g. a JNI exception). This function does not log any errors or exceptions.
  static bool MethodExists(JNIEnv* env, jclass class_ref,
      const char* method_str, const char* method_signature);

  /// Returns a global JNI reference to the class specified by class_str into class_ref.
  /// The returned reference must eventually be freed by calling FreeGlobalRef() (or have
  /// the lifetime of the impalad process).
  /// Catches Java exceptions and converts their message into status.
  static Status GetGlobalClassRef(
      JNIEnv* env, const char* class_str, jclass* class_ref) WARN_UNUSED_RESULT;

  /// Creates a global reference from a local reference returned into global_ref.
  /// The returned reference must eventually be freed by calling FreeGlobalRef() (or have
  /// the lifetime of the impalad process).
  /// Catches Java exceptions and converts their message into status.
  static Status LocalToGlobalRef(JNIEnv* env, jobject local_ref,
      jobject* global_ref) WARN_UNUSED_RESULT;

  /// Templated wrapper for jobject subclasses (e.g. jclass, jarray). This is necessary
  /// because according to
  /// http://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/types.html:
  ///   class _jobject {};
  ///   class _jclass : public _jobject {};
  ///   ...
  ///   typedef _jobject *jobject;
  ///   typedef _jclass *jclass;
  /// This mean jobject* is actually _jobject**, so we need the reinterpret_cast in order
  /// to use a subclass like _jclass**. This is safe in this case because the returned
  /// subclass is known to be correct.
  template <typename jobject_subclass>
  static Status LocalToGlobalRef(
      JNIEnv* env, jobject local_ref, jobject_subclass* global_ref) {
    return LocalToGlobalRef(env, local_ref, reinterpret_cast<jobject*>(global_ref));
  }

  static jmethodID throwable_to_string_id() { return throwable_to_string_id_; }
  static jmethodID throwable_to_stack_trace_id() { return throwable_to_stack_trace_id_; }

  /// Returns true if an embedded JVM is initialized, false otherwise.
  static bool is_jvm_inited() { return jvm_inited_; }

  /// Global reference to java JniUtil class
  static jclass jni_util_class() { return jni_util_cl_; }

  /// Global reference to InternalException class.
  static jclass internal_exc_class() { return internal_exc_cl_; }

  /// Returns the error message for 'e'. If no exception, returns Status::OK
  /// log_stack determines if the stack trace is written to the log
  /// prefix, if non-empty will be prepended to the error message.
  static Status GetJniExceptionMsg(JNIEnv* env, bool log_stack = true,
      const std::string& prefix = "") WARN_UNUSED_RESULT;

  /// Populates 'result' with a list of memory metrics from the Jvm. Returns Status::OK
  /// unless there is an exception.
  static Status GetJvmMemoryMetrics(const TGetJvmMemoryMetricsRequest& request,
      TGetJvmMemoryMetricsResponse* result) WARN_UNUSED_RESULT;

  /// Populates 'result' with information about live JVM threads. Returns
  /// Status::OK unless there is an exception.
  static Status GetJvmThreadsInfo(const TGetJvmThreadsInfoRequest& request,
      TGetJvmThreadsInfoResponse* result) WARN_UNUSED_RESULT;

  /// Gets JMX metrics of the JVM encoded as a JSON string.
  static Status GetJMXJson(TGetJMXJsonResponse* result) WARN_UNUSED_RESULT;

  /// Loads a method whose signature is in the supplied descriptor. Returns Status::OK
  /// and sets descriptor->method_id to a JNI method handle if successful, otherwise an
  /// error status is returned.
  static Status LoadJniMethod(JNIEnv* jni_env, const jclass& jni_class,
      JniMethodDescriptor* descriptor) WARN_UNUSED_RESULT;

  /// Same as LoadJniMethod(...), except that this loads a static method.
  static Status LoadStaticJniMethod(JNIEnv* jni_env, const jclass& jni_class,
      JniMethodDescriptor* descriptor) WARN_UNUSED_RESULT;

  /// Utility methods to avoid repeating lots of the JNI call boilerplate.
  /// New code should prefer using JniCall() directly for better clarity.
  static Status CallJniMethod(
      const jobject& obj, const jmethodID& method) WARN_UNUSED_RESULT {
    return JniCall(method).on_instance(obj).Call();
  }

  static Status CallStaticJniMethod(
      const jclass& cls, const jmethodID& method) WARN_UNUSED_RESULT {
    return JniCall(method).on_class(cls).Call();
  }

  template <typename T>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method, const T& arg) {
    return JniCall(method).on_instance(obj).with_thrift_arg(arg).Call();
  }

  template <typename T, typename R>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method, const T& arg, R* response) {
    return JniCall(method).on_instance(obj).with_thrift_arg(arg).Call(response);
  }

  template <typename R>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method, R* response) {
    return JniCall(method).on_instance(obj).Call(response);
  }

 private:
  // Set in Init() once the JVM is initialized.
  static bool jvm_inited_;
  static jclass jni_util_cl_;
  static jclass internal_exc_cl_;
  static jmethodID throwable_to_string_id_;
  static jmethodID throwable_to_stack_trace_id_;
  static jmethodID get_jvm_metrics_id_;
  static jmethodID get_jvm_threads_id_;
  static jmethodID get_jmx_json_;
};


template<class T>
inline Status JniCall::Call(T* result) {
  RETURN_IF_ERROR(status_);
  DCHECK(instance_ || class_);

  // Even if the function takes no arguments, it's OK to pass an array here.
  // The JNI API doesn't take a length and just assumes that you've passed
  // an appropriate number of elements.
  jvalue args_array[1];
  args_array[0].l = arg_;
  jobject ret;
  if (class_) {
    ret = env_->CallStaticObjectMethodA(class_, method_, args_array);
  } else {
    ret = env_->CallObjectMethodA(instance_, method_, args_array);
  }
  RETURN_ERROR_IF_EXC(env_);
  RETURN_IF_ERROR(ObjectToResult(ret, result));
  return Status::OK();
}

inline Status JniCall::ObjectToResult(jobject obj, std::string* result) {
  jboolean is_copy;
  DCHECK(obj) << "Call returned unexpected null Sttring instance";
  jstring jstr = static_cast<jstring>(obj);
  const char *str = env_->GetStringUTFChars(jstr, &is_copy);
  RETURN_ERROR_IF_EXC(env_);
  *result = str;
  env_->ReleaseStringUTFChars(jstr, str);
  RETURN_ERROR_IF_EXC(env_);
  return Status::OK();
}

}

#endif
