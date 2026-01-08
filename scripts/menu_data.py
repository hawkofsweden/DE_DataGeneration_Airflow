import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()

# Coffee Shop Menu Configuration
coffee_products = {
    "Hot Coffees": [
        "Caffe Latte", "Cappuccino", "Caramel Macchiato", "Caffe Mocha", 
        "White Chocolate Mocha", "Americano", "Flat White", "Espresso", 
        "Cortado", "Filter Coffee", "Turkish Coffee"
    ],
    "Cold Coffees": [
        "Iced Latte", "Iced Americano", "Iced Caramel Macchiato", "Iced Mocha", 
        "Cold Brew", "Iced White Chocolate Mocha", "Iced Flat White", 
        "Nitro Cold Brew", "Iced Shaken Espresso"
    ],
    "Bakery & Pastries": [
        "Chocolate Croissant", "Butter Croissant", "Blueberry Muffin", 
        "Chocolate Chip Cookie", "Chocolate Brownie", "Lemon Cheesecake", 
        "Carrot Cake", "Tiramisu", "Apple Pie", "Cinnamon Roll"
    ],
    "Breakfast & Sandwiches": [
        "Cheese & Tomato Toast", "Egg & Bacon Sandwich", "Chicken Caesar Wrap", 
        "Smoked Salmon Bagel", "Mozzarella Panini", "Tuna Melt Sandwich", 
        "Oatmeal Bowl", "Fruit Parfait"
    ],
    "Non-Coffee & Teas": [
        "Hot Chocolate", "Chai Latte", "Matcha Latte", "English Breakfast Tea", 
        "Earl Grey Tea", "Green Tea", "Hibiscus Iced Tea", "Peach Iced Tea", 
        "Lemonade", "Iced Matcha Latte"
    ],
    "Beans & Merch": [
        "Ethiopia Whole Bean (250g)", "Colombia Whole Bean (250g)", 
        "Espresso Roast (250g)", "House Blend (250g)", "White Ceramic Mug", 
        "Tumbler (16oz)", "French Press", "V60 Dripper"
    ]
}

def menu_data(coffee_products):
    menu_items = []
    product_id_counter = 1

    for category, products in coffee_products.items():
        for name in products:
            price = round(random.uniform(3.0, 9.0), 2)
            menu_items.append({
                'product_id': product_id_counter,
                'product_name': name,
                'product_category': category,
                'unit_price': price,
                'product_description': fake.text()
            })
            product_id_counter += 1
    return pd.DataFrame(menu_items)

if __name__ == "__main__":
    OUTPUT_DIR = "/opt/airflow/outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    menu = menu_data(coffee_products)
    file_name = f"menu_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    full_path = os.path.join(OUTPUT_DIR, file_name)
    
    menu.to_csv(full_path, index=False)
    print(f"Menu Data Generated and Saved to {full_path}")
    print(menu.head())
