package utils

func Assert(condition bool, msg ...string) {
	if !condition {
		if len(msg) > 0 {
			panic("assertin failed: " + msg[0])
		}
		panic("assertion failed!")
	}
}
