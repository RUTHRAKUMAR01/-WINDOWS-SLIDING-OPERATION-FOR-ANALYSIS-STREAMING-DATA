import csv
from faker import Faker
import datetime
import random
import string
import pandas as pd
from datetime import datetime, timedelta

fake = Faker()
with open('input_name_1.csv', mode='w', newline='') as file:
    #Create CSV writer object
    writer = csv.writer(file)
    #Write header row
    writer.writerow(['Invoice', 'StockCode', 'Name', 'Quantity', 'InvoiceDate','Unit'])
    customer_ids = set()
    i=0
    for i in range (5000):
    start_timestamp = datetime (2024, 3, 24, 0, 0, 0)
    Invoice = fake.random_int (min=10000, max=50000)
    StockCode = fake.random_int (min=10000, max-99999)
    Name = fake.name()
    Quantity = random.randint (1, 100)
    InvoiceDate = fake.date_time_this_decade()
    UnitPrice = round(random.uniform (0.1, 100), 2)
    CustomerId = fake.random_int (min=1000, max=6000)
    Country = fake.country()
    InvoiceTimestamp = start_timestamp + timedelta(minutes=5*i)
    if CustomerId not in customer ids:
        customer_ids.add(CustomerId)
        writer.writerow ([Invoice, StockCode, Name, Quantity, InvoiceDate, Unit])
        i=i+1
print("Success")




      
