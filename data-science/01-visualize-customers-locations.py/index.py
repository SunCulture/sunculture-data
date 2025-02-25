import streamlit as st
import pandas as pd
import pydeck as pdk  # For interactive maps

def preprocess_agro_delaers_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and extracts latitude and longitude from the 'location' column."""
    df = df.dropna(subset=['location'])
    df = df[df['location'].str.strip() != ""]

    # Extract lat/lon from POINT(x y) format
    df['location'] = df['location'].str.extract(r'POINT\((.*?)\)')[0]
    split_values = df['location'].str.split(' ', expand=True)

    processed_df = pd.DataFrame()
    processed_df['lat'] = pd.to_numeric(split_values[1], errors='coerce')  # y-coordinates (lat)
    processed_df['lon'] = pd.to_numeric(split_values[0], errors='coerce')  # x-coordinates (lon)

    # Drop invalid rows (NaN values and (0,0) coordinates)
    processed_df = processed_df.dropna(subset=['lat', 'lon'])
    processed_df = processed_df[(processed_df['lat'] != 0) & (processed_df['lon'] != 0)]

    processed_df['shop_name'] = df['shop_name']
    processed_df['shop_type'] = "Agro-Delear"

    return processed_df
def preprocess_customers_data(df: pd.DataFrame) -> pd.DataFrame:
    """Uses pre-cleaned latitude & longitude columns directly for existing shops."""
    df = df.dropna(subset=['latitude', 'longitude'])
    df = df[(df['latitude'] != 0) & (df['longitude'] != 0)]

    processed_df = pd.DataFrame()
    processed_df['lat'] = df['latitude']
    processed_df['lon'] = df['longitude']
    processed_df['shop_name'] = df['name']
    processed_df['shop_type'] = "Customers"

    return processed_df
# Load CSV
#agro_dealers_file_path = "../../../../../../all_shops_02202025.csv"
agro_dealers_file_path = "https://raw.githubusercontent.com/SunCulture/sunculture-data/refs/heads/rodgers-dev/data-sets/all_shops_02202025.csv"
#customers_file_path = "../../../../../../customers.csv"
customers_file_path = "https://raw.githubusercontent.com/SunCulture/sunculture-data/refs/heads/rodgers-dev/data-sets/customers.csv"

agro_delears = pd.read_csv(agro_dealers_file_path)
customers = pd.read_csv(customers_file_path)

# Process location data
agro_delaers_data = preprocess_agro_delaers_data(agro_delears)
customers_data = preprocess_customers_data(customers)

# Combine both datasets
map_data = pd.concat([agro_delaers_data, customers_data], ignore_index=True)
map_data["lat"] = pd.to_numeric(map_data["lat"], errors="coerce")
map_data["lon"] = pd.to_numeric(map_data["lon"], errors="coerce")

# Streamlit UI
st.title("Shop Locations: Existing vs Potential")
st.write("Filter by shop type and shop name to visualize locations.")

# **Shop Type Filter**
shop_type_options = ["All", "Potential Shop", "Existing Shop"]
selected_shop_types = st.multiselect("Select Shop Type", shop_type_options, default="All")

# **Shop Name Filter**
shop_name_options = ["All"] + sorted(map_data["shop_name"].unique().tolist())
selected_shops = st.multiselect("Select Shop(s)", shop_name_options, default="All")

# Apply filters
filtered_data = map_data
if "All" not in selected_shop_types:
    filtered_data = filtered_data[filtered_data["shop_type"].isin(selected_shop_types)]

if "All" not in selected_shops:
    filtered_data = filtered_data[filtered_data["shop_name"].isin(selected_shops)]

# Define color mapping
color_map = {
    "Agro-Delear": [255, 0, 0, 200],  # Red for Agro-Dealers
    "Customers": [0, 0, 255, 200]     # Blue for Customers
}
filtered_data["color"] = filtered_data["shop_type"].map(color_map)

# Create layers for Pydeck
layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered_data,
    get_position=["lon", "lat"],
    get_color="color",
    get_radius=600,  # Increase marker size
    pickable=True,
)

# Configure the map view
view_state = pdk.ViewState(
    latitude=filtered_data["lat"].mean(),
    longitude=filtered_data["lon"].mean(),
    zoom=6,
    pitch=0,
)

# Render the map
st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "{shop_name} ({shop_type})"},  # Show shop type on hover
))

# Show filtered data table
st.dataframe(filtered_data)