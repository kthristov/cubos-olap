package utad.utils

import java.sql.Timestamp

object TimeUtils {

	/**
	  * Return timestamp hour
	  * @param time Timestamp which hour is wanted
	  * @return current hour
	  */
	def roundTimestampToHour(time: Timestamp): Int =
		time.toLocalDateTime.getHour

}
