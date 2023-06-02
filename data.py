import pandas as pd
from datetime import datetime


hotel_df = pd.read_csv('hotel-booking.csv')
customer_df = pd.read_csv('customer-reservations.csv')

hotel_df = hotel_df[(hotel_df['booking_status'] == 0)]
hotel_df['cost'] = (hotel_df['stays_in_weekend_nights'] + hotel_df['stays_in_week_nights']) * hotel_df['avg_price_per_room']
hotel_df = hotel_df.drop(['hotel', 'booking_status', 'lead_time', 'arrival_date_week_number', 'arrival_date_day_of_month', 'stays_in_weekend_nights',
                          'stays_in_week_nights', 'market_segment_type', 'country', 'avg_price_per_room', 'email'], axis=1)

hotel_df['arrival_month'] = hotel_df['arrival_month'].apply(lambda x: datetime.strptime(x, '%B').month)

customer_df = customer_df[(customer_df['booking_status'] == 'Not_Canceled')]
customer_df['cost'] = (customer_df['stays_in_weekend_nights'] + customer_df['stays_in_week_nights']) * customer_df['avg_price_per_room']
customer_df = customer_df.drop(['Booking_ID', 'stays_in_weekend_nights', 'stays_in_week_nights', 'lead_time', 'arrival_date', 'market_segment_type',
                                'avg_price_per_room','booking_status'], axis=1)

final_df = pd.concat([hotel_df, customer_df], ignore_index=True)
print((final_df))
final_df.to_csv('preprocessed.csv', index=False)