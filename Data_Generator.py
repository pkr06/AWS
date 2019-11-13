import csv
from faker import Faker
import datetime
from faker import providers

class Data_Generator:
    def __init__(self):
        pass


    def _personal_data(self, records = 10000):

        headers = ["Email Id", "Prefix", "Name", "Birth Date", "Phone Number", "Additional Email Id",
                   "Address", "Zip Code", "City", "State", "Country"]

        fake = Faker('en_US')
        fake1 = Faker('en_GB')  # To generate phone numbers
        data_ = []
        with open("People_data.csv", 'wt') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=headers)
            writer.writeheader()
            for i in range(records):
                full_name = fake.name()
                FLname = full_name.split(" ")
                Fname = FLname[0]
                Lname = FLname[1]
                domain_name = "@testDomain.com"
                userId = Fname + "." + Lname + domain_name

                dat_row = {
                    "Email Id": userId,
                    "Prefix": fake.prefix(),
                    "Name": fake.name(),
                    "Birth Date": fake.date(pattern="%Y-%m-%d %H:%M:%S.%f", end_datetime=datetime.date(2000, 1, 1)),
                    "Phone Number": fake1.phone_number(),
                    "Additional Email Id": fake.email(),
                    "Address": fake.address(),
                    "Zip Code": fake.zipcode(),
                    "City": fake.city(),
                    "State": fake.state(),
                    "Country": fake.country(),
                }
                data_.append(dat_row)
                writer.writerow(dat_row)
        return data_

    def _trading_data(self, records = 10000):

        headers = ["ccy", "timestamp", "open", "high", "low", "close", "volume"]

        fake = Faker('en_US')
        fake1 = Faker('en_GB')
        ccy_providers =  fake.add_provider( providers.currency.Provider)
        data_ = []
        with open("price_data.csv", 'wt') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=headers)
            writer.writeheader()
            for i in range(records):
                ccy = fake.name()

                dat_row = {
                    "ccy": ccy,
                    "timestamp": fake.date(pattern="%Y-%m-%d %H:%M:%S.%f", end_datetime=datetime.date(2000, 1, 1)),
                    "open": fake.randomize_nb_elements(100),
                    "high": fake.randomize_nb_elements(100),
                    "low": fake.randomize_nb_elements(100),
                    "close": fake.randomize_nb_elements(100),
                    "volume": fake.randomize_nb_elements(10000),

                    "Text": fake.word(),
                }
                data_.append(dat_row)
                writer.writerow(dat_row)
        return data_

if __name__ == '__main__':
    d = Data_Generator()
    dat = d._personal_data()
    print("CSV generation complete!")
    print (dat)