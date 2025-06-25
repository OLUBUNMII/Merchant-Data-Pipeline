from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os
os.makedirs("../data", exist_ok=True)

fake = Faker()
# function to generate a simulated transaction
def generate_transaction():
    merchant_id = f"M{random.randint(1000, 9999)}" #Generates a fake Merchant ID between 1000 and 9999
    terminal_id = f"T{random.randint(100, 999)}" #Generates a fake Terminal ID between 100 and 999
    transaction_type = random.choice(["payment", "withdrawal", "transfer"]) #Picks a random transaction type for each transaction. Wether it was a payment, withdrawal or a transfer
    amount = round(random.uniform(10.0, 5000.0), 2) #Picks a random float for an amount between 10 naira and 5,000 naira then rounds it up to 2 decimal places
    status = random.choice(["successful", "failed"]) # randomly assigns the statuses successful or failed to a transaction
    timestamp = fake.date_time_between(start_date="-30d", end_date="now") #Generates random datetime from the last 30days up until now.

    return {
        "merchant_id": merchant_id,
        "terminal_id": terminal_id,
        "timestamp": timestamp,
        "transaction_type": transaction_type,
        "amount": amount,
        "status": status
    }

def generate_transactions_batch(n=1000):   #generates 1000 transactions by default
    data = [generate_transaction() for _ in range(n)] #list of 1000 fake records

    #-----Edge cases to simulate realism of data-----
    #Inject missing values into 1% of the data (10 out 1000)
    for row in random.sample(data, int(0.01 * n)):  
        field = random.choice(["amount", "status", "timestamp"]) #for each 1 of the 10, randomly remove either amount, status or timestamp
        row[field] = pd.NA #pd.NA = pandas version of None89

    df = pd.DataFrame(data)  #Puts the list in a table

    #Inject Duplicates (1% of data)
    duplicates = df.sample(frac=0.01, random_state=42) #Randomly select 1% of the data. random_state=42 ensures reproducibilty; you'd get a random sample every time you run th escript
    df = pd.concat([df, duplicates], ignore_index=True) #Adds sampled duplicates back into dataframe (df)
    df = df.sample(frac=1).reset_index(drop=True) #shuffle everything

    #Inject out-of-order arrivals
    shuffle_frac = 0.05 #5% of data to be out of order
    #Split into ordered and unordered portions
    ordered_part = df.sort_values(by="timestamp").reset_index(drop=True) #sort all transactions by time
    unordered_indices = random.sample(range(len(ordered_part)), int(shuffle_frac * len(ordered_part))) #Randomly select 5% of the dataset to mess up (out of order)
    unordered_part = ordered_part.loc[unordered_indices].sample(frac=1).reset_index(drop=True) #Shuffle the 5% so they're in the wrong order
    #Re-Inject out-of-order rows randomly into the dataset
    ordered_part.drop(index=unordered_indices, inplace=True) #remove 5% from the clean, sorted dataset because the messsed-up versions would be put back in.
    df = pd.concat([ordered_part, unordered_part], ignore_index=True) #Combine the clean data and the messed up rows
    df = df.sample(frac=1).reset_index(drop=True) #shuffle everything to simulate out-of-order realism
    output_path = os.path.join(os.path.dirname(__file__), "..", "data", "transactions.csv")
    df.to_csv(output_path, index=False) #save as csv in /data folder. "index=false"- the datframe row numbers are not includeed in the file.

if __name__ == "__main__":
    print("Generating transactions...")
    generate_transactions_batch()
    print("Done, check 'data/' for 'transaction.csv'")