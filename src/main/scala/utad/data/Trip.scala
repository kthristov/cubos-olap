package utad.data

case class Trip(
	               vendor_id: Long,
	               pickup_dt: java.sql.Timestamp,
	               dropoff_dt: java.sql.Timestamp,
	               passengers: Integer,
	               distance: Float,
	               ratecode_id: Integer,
	               store_fwd_flag: String,
	               pu_location_id: Integer,
	               do_location_id: Integer,
	               payment_type: Integer,
	               fare_amount: Float,
	               extra: Float,
	               mta_max: Float,
	               tip_amount: Float,
	               tolls_amount: Float,
	               improvement_surcharge: Float,
	               total_amount: Float
               )
