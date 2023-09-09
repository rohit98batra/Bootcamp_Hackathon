import random
import time
import datetime

# Sample ad_ids, user_ids, product_ids, and revenues
ad_ids = list(range(1001, 1021))
user_ids = [
    "U9239", "U4534", "U4068", "U6347", "U8632",
    "U9706", "U9237", "U1653", "U4897", "U4009",
    "U4797", "U4439", "U0372", "U1429", "U7586",
    "U9797", "U7221", "U9615", "U9764", "U4991",
    "U4824", "U4075", "U1227", "U5411", "U8129",
    "U8420", "U8500", "U7619", "U8732", "U0374",
    "U7802", "U3632", "U7504", "U6266", "U4731",
    "U9816", "U1188", "U8948", "U3009", "U0422",
    "U9133", "U5745", "U5674", "U8983", "U3567",
    "U6297", "U3380", "U7796", "U3551", "U5373",
    "U9118", "U1660", "U0535", "U6634", "U3563",
    "U8939", "U8171", "U8441", "U1089", "U4453",
    "U9248", "U0162", "U6300", "U9765", "U7533",
    "U4898", "U0778", "U0719", "U7972", "U7969",
    "U0395", "U9298", "U6746", "U2377", "U3959",
    "U1916", "U0235", "U5170", "U5476", "U9770",
    "U9431", "U9279", "U0421", "U6429", "U8601",
    "U8911", "U2362", "U1667", "U4594", "U3541",
    "U1209", "U6496", "U4909", "U6341", "U2689",
    "U8661", "U0550", "U2709", "U2241", "U8835"
]


# Function to generate a random Ad Conversion event
def generate_ad_conversion():
    ad_id = random.choice(ad_ids)
    user_id = random.choice(user_ids)
    conversion_timestamp = (datetime.datetime.now() + datetime.timedelta(days=random.randint(1, 2),hours=random.randint(1, 24))).strftime("%Y-%m-%d %H:%M:%S")
    product_id = f"P{ad_id-500:03d}"
    revenue = round(random.uniform(10.0, 500.0), 2)  # Random revenue between 10 and 500
    
    return {
        "ad_id": ad_id,
        "user_id": user_id,
        "conversion_timestamp": conversion_timestamp,
        "product_id": product_id,
        "revenue": revenue
    }

# Main loop to generate and print mock data every 10 seconds
while True:
    ad_conversion_data = generate_ad_conversion()
    print(ad_conversion_data)
    time.sleep(1)  # Adjust the sleep interval as needed
