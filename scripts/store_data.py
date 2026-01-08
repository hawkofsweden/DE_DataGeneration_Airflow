import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()

# Configuration
store_types = ['High-Street', 'Mall', 'Pop-up', 'Drive-Thru']
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

def generate_store_data(num_stores=50):
    """
    Generates store data with related country/city consistency.
    """
    store_locations = []
    for _ in range(num_stores):
        country = random.choice(list(european_locations.keys()))
        city = random.choice(european_locations[country])
        store_locations.append((country, city))

    store_df = pd.DataFrame({
        'store_id': range(1, num_stores + 1),
        'store_name': [fake.company() for _ in range(num_stores)],
        'store_type': [random.choices(store_types, weights=[1, 1, 2, 1], k=1)[0] for _ in range(num_stores)],
        'store_country': [loc[0] for loc in store_locations],
        'store_city': [loc[1] for loc in store_locations],
        'store_address': [fake.address().replace('\n', ', ') for _ in range(num_stores)],
        'store_open_date': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_stores)],
        'store_phone': [fake.phone_number() for _ in range(num_stores)],
        'store_area': [random.choice(['Small', 'Medium', 'Large']) for _ in range(num_stores)],
        'store_manager': [fake.name() for _ in range(num_stores)],
    })
    return store_df

if __name__ == "__main__":
    OUTPUT_DIR = "/opt/airflow/outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stores = generate_store_data()
    file_name = f"store_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    full_path = os.path.join(OUTPUT_DIR, file_name)
    
    stores.to_csv(full_path, index=False) 
    print(f"Store Data Generated and Saved to {full_path}")
    print(stores.head())
