package HyperLogLog

func CountTrailingZeros(value uint32, precision int) uint8 {
	for i := 0; i < 32-precision; i++ {
		if ((value >> i) & 1) == 1 {
			return uint8(i)
		}
	}
	return uint8(32 - precision)
}
