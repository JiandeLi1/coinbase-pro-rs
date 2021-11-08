use mysql::;
use mysql::prelude::;
use chrono::prelude::;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use coinbase_pro_rs::{structs::wsfeed::, WSFeed, WS_SANDBOX_URL};
use futures::{StreamExt, TryStreamExt};
use std::*;

#[tokio::main]
async fn main() {
    let url = "mysql://root:1234@localhost:3306/myblog";
        let opts = Opts::from_url(url).unwrap();
        let pool = Pool::new(opts).unwrap();
        let inf=std::f32::INFINITY as usize;

    // conn.query_drop(
    // r"CREATE TEMPORARY TABLE payment (
    //     customer_id int not null,
    //     amount int not null,
    //     account_name text
    // )").unwrap();
    loop{
        let stream = WSFeed::connect(WS_SANDBOX_URL, &["BTC-USD"], &[ChannelType::Ticker])
        .await
        .unwrap();
        stream.take(inf)
        .try_for_each(|msg| async{
            let mut conn = pool.get_conn().unwrap();
            match msg {
                Message::Ticker(Full)=>conn.exec_drop("INSERT INTO test (product_id, date, price) VALUES (:product_id, :date, :price)", params!{
                    "product_id"=>Full.product_id(),
                    "date"=>NaiveDate::from_ymd(
                        Full.time().unwrap().year(),
                        Full.time().unwrap().month(), 
                        Full.time().unwrap().day()
                ).andhms( Full.time().unwrap().hour(),
                        Full.time().unwrap().minute(),
                        Full.time().unwrap().second()),
                    "price"=>Full.price(),
                }).unwrap(),
                Message::Error { mut message } =>println!("Error {:?}", message),
                Message::InternalError() => panic!("internal_error"),
                other =>(),
            };
            Ok(())
        })
        .await
        .expect("stream fail");
    }
}


fn print_typeof<T>(: &T) {
    println!("{}", std::any::type_name::<T>())
}
