package mqtt

import (
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
)

const (
	logKey = "[mqtt]"
)

///
/// logger
///

func LogDebug(args ...interface{}) {
	log.Debugf("%s %s", logKey, fmt.Sprint(args...))
}

func LogInfo(args ...interface{}) {
	log.Infof("%s %s", logKey, fmt.Sprint(args...))
}

func LogWarn(args ...interface{}) {
	log.Warnf("%s %s", logKey, fmt.Sprint(args...))
}

func LogError(args ...interface{}) {
	log.Errorf("%s %s", logKey, fmt.Sprint(args...))
}

func LogFatal(args ...interface{}) {
	log.Fatalf("%s %s", logKey, fmt.Sprint(args...))
}

///
/// logger
///

func LogDebugf(format string, args ...interface{}) {
	log.Debugf("%s %s", logKey, fmt.Sprintf(format, args...))
}

func LogInfof(format string, args ...interface{}) {
	log.Infof("%s %s", logKey, fmt.Sprintf(format, args...))
}

func LogWarnf(format string, args ...interface{}) {
	log.Warnf("%s %s", logKey, fmt.Sprintf(format, args...))
}

func LogErrorf(format string, args ...interface{}) {
	log.Errorf("%s %s", logKey, fmt.Sprintf(format, args...))
}

func LogFatalf(format string, args ...interface{}) {
	log.Fatalf("%s %s", logKey, fmt.Sprintf(format, args...))
}

///
/// ErrorLogger
///

type ErrorLogger struct{}

func (ErrorLogger) Println(v ...interface{}) {
	log.Error(v...)
}

func (ErrorLogger) Printf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

///
/// CriticalLogger
///

type CriticalLogger struct{}

func (CriticalLogger) Println(v ...interface{}) {
	log.Fatal(v...)
}

func (CriticalLogger) Printf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

///
/// WarnLogger
///

type WarnLogger struct{}

func (WarnLogger) Println(v ...interface{}) {
	log.Warn(v...)
}

func (WarnLogger) Printf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

///
/// DebugLogger
///

type DebugLogger struct{}

func (DebugLogger) Println(v ...interface{}) {
	log.Debug(v...)
}

func (DebugLogger) Printf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}
