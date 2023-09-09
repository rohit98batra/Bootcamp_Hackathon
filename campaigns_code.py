import csv
import random
import datetime

# List of 20 unique product names
product_names = [
    "Laptop", "Smartphone", "Tablet", "Camera", "Headphones",
    "Television", "Refrigerator", "Washing Machine", "Microwave", "Coffee Maker",
    "Gaming Console", "Fitness Tracker", "Blender", "Toaster", "Vacuum Cleaner",
    "Air Purifier", "Soundbar", "Printer", "Router", "External Hard Drive"
]

# List of 20 unique campaign names
campaign_names = [
    "Back-to-School", "Summer Sale", "Fall Fashion", "Holiday Special", "Tech Expo",
    "Home Renovation", "Black Friday", "Cyber Monday", "Winter Clearance", "Spring Savings",
    "Easter Deals", "Valentine's Day", "Mother's Day", "Father's Day", "New Year's Celebration",
    "Super Sale", "Outdoor Adventure", "Health & Wellness", "Backyard BBQ", "Fashion Forward"
]

# Function to generate a unique product name
def generate_unique_product(existing_products):
    while True:
        product = random.choice(product_names)
        if product not in existing_products:
            existing_products.add(product)
            return product

# Function to generate a unique campaign name
def generate_unique_campaign(existing_campaigns):
    while True:
        campaign = random.choice(campaign_names)
        if campaign not in existing_campaigns:
            existing_campaigns.add(campaign)
            return campaign

# Sample data for the Campaigns dimension table
campaigns_data = []
existing_campaigns = set()
existing_products = set()

for ad_id in range(1001, 1021):
    product = generate_unique_product(existing_products)
    campaign = generate_unique_campaign(existing_campaigns)
    start_date = (datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 60))).strftime("%Y-%m-%d")
    end_date = (datetime.datetime.now() + datetime.timedelta(days=random.randint(30, 120))).strftime("%Y-%m-%d")

    campaigns_data.append({
        "ad_id": ad_id,
        "campaign": campaign,
        "product": product,
        "target_start_date": start_date,
        "target_end_date": end_date
    })

# Define the CSV file path
csv_file_path = "campaigns.csv"

# Write the data to a CSV file
with open(csv_file_path, mode='w', newline='') as csv_file:
    fieldnames = ["ad_id", "campaign", "product", "target_start_date", "target_end_date"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Write the sample data
    writer.writerows(campaigns_data)

print(f"CSV file '{csv_file_path}' has been created with Campaigns dimension data.")
