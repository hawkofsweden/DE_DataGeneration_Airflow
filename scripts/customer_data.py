import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Configuration
european_locations = {
    "Turkey": ["Istanbul", "Ankara", "Izmir", "Bursa", "Antalya", "Adana", "Gaziantep", "Konya"],
    "Germany": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Stuttgart", "Dusseldorf", "Dortmund"],
    "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier"],
    "United Kingdom": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Edinburgh", "Leeds", "Bristol"],
    "Italy": ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Malaga", "Murcia", "Palma"],
    "Netherlands": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven", "Tilburg", "Groningen"],
    "Belgium": ["Brussels", "Antwerp", "Ghent", "Charleroi", "Liège", "Bruges", "Namur"],
    "Switzerland": ["Zurich", "Geneva", "Basel", "Lausanne", "Bern", "Winterthur", "Lucerne"],
    "Austria": ["Vienna", "Graz", "Linz", "Salzburg", "Innsbruck", "Klagenfurt", "Villach"],
    "Portugal": ["Lisbon", "Porto", "Amadora", "Braga", "Setúbal", "Coimbra", "Funchal"],
    "Sweden": ["Stockholm", "Gothenburg", "Malmö", "Uppsala", "Västerås", "Örebro", "Linköping"],
    "Norway": ["Oslo", "Bergen", "Stavanger", "Trondheim", "Drammen", "Fredrikstad", "Kristiansand"],
    "Denmark": ["Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg", "Randers", "Kolding"],
    "Poland": ["Warsaw", "Kraków", "Łódź", "Wrocław", "Poznań", "Gdańsk", "Szczecin", "Bydgoszcz"],
    "Czech Republic": ["Prague", "Brno", "Ostrava", "Plzeň", "Liberec", "Olomouc", "Ústí nad Labem"],
    "Hungary": ["Budapest", "Debrecen", "Szeged", "Miskolc", "Pécs", "Győr", "Nyíregyháza"],
    "Greece": ["Athens", "Thessaloniki", "Patras", "Heraklion", "Larissa", "Volos", "Rhodes"],
    "Ireland": ["Dublin", "Cork", "Limerick", "Galway", "Waterford", "Drogheda", "Dundalk"],
    "Finland": ["Helsinki", "Espoo", "Tampere", "Vantaa", "Oulu", "Turku", "Jyväskylä"]
}

def generate_customer_data(num_rows=300):
    """
    Generates a DataFrame of fake customer data with consistent location logic.
    """
    customer_locations = []
    for _ in range(num_rows):
        country = random.choice(list(european_locations.keys()))
        city = random.choice(european_locations[country])
        customer_locations.append((country, city))

    customer_df = pd.DataFrame({
        'customer_id': range(1, num_rows + 1),
        'customer_birth_date': [fake.date_of_birth(minimum_age=18, maximum_age=65) for _ in range(num_rows)],
        'customer_name': [fake.name() for _ in range(num_rows)],
        'customer_email': [fake.email() for _ in range(num_rows)],
        'customer_phone': [fake.phone_number() for _ in range(num_rows)],
        'customer_address': [fake.address().replace('\n', ', ') for _ in range(num_rows)],
        'customer_country': [loc[0] for loc in customer_locations],
        'customer_city': [loc[1] for loc in customer_locations],
        'customer_zip': [fake.zipcode() for _ in range(num_rows)],
    })
    return customer_df


if __name__ == "__main__":
    OUTPUT_DIR = "/opt/airflow/data_output"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    customers = generate_customer_data()
    file_name = f"customer_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    full_path = os.path.join(OUTPUT_DIR, file_name)
    
    customers.to_csv(full_path, index=False)
    print(f"Customer Data Generated and Saved to {full_path}")
    print(customers.head(25))
