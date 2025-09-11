import streamlit as st
import pandas as pd
import pydeck as pdk
import numpy as np
from scipy.spatial import ConvexHull
from typing import Tuple, Dict, List, Optional

# Define geographic bounds for Kenya and Uganda
KENYA_UGANDA_BOUNDS = {
    "min_lon": 29.0,  # Westernmost point (Uganda/DRC border)
    "max_lon": 42.0,  # Easternmost point (Kenya/Somalia border)
    "min_lat": -5.0,  # Southernmost point (Tanzania border)
    "max_lat": 5.0,   # Northernmost point (South Sudan border)
}

def preprocess_agro_dealers_data(df: pd.DataFrame) -> pd.DataFrame:
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

    # Filter to only include Kenya and Uganda locations
    processed_df = processed_df[
        (processed_df['lon'] >= KENYA_UGANDA_BOUNDS["min_lon"]) &
        (processed_df['lon'] <= KENYA_UGANDA_BOUNDS["max_lon"]) &
        (processed_df['lat'] >= KENYA_UGANDA_BOUNDS["min_lat"]) &
        (processed_df['lat'] <= KENYA_UGANDA_BOUNDS["max_lat"])
    ]

    processed_df['shop_name'] = df.loc[processed_df.index, 'shop_name']
    processed_df['shop_type'] = "Agro-Dealer"

    return processed_df

def preprocess_customers_data(df: pd.DataFrame) -> pd.DataFrame:
    """Uses pre-cleaned latitude & longitude columns directly for existing shops."""
    # Convert to numeric, handling strings and errors
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    df = df.dropna(subset=['latitude', 'longitude'])
    df = df[(df['latitude'] != 0) & (df['longitude'] != 0)]

    # Filter to only include Kenya and Uganda locations
    df = df[
        (df['longitude'] >= KENYA_UGANDA_BOUNDS["min_lon"]) &
        (df['longitude'] <= KENYA_UGANDA_BOUNDS["max_lon"]) &
        (df['latitude'] >= KENYA_UGANDA_BOUNDS["min_lat"]) &
        (df['latitude'] <= KENYA_UGANDA_BOUNDS["max_lat"])
    ]

    processed_df = pd.DataFrame()
    processed_df['lat'] = df['latitude']
    processed_df['lon'] = df['longitude']
    processed_df['shop_name'] = df['name']
    processed_df['shop_type'] = "Customer"

    return processed_df

def preprocess_ssc_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processes SSC data with lat/lon columns."""
    # Convert to numeric, handling strings and errors
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    
    df = df.dropna(subset=['lat', 'lon'])
    df = df[(df['lat'] != 0) & (df['lon'] != 0)]

    # Filter to only include Kenya and Uganda locations
    df = df[
        (df['lon'] >= KENYA_UGANDA_BOUNDS["min_lon"]) &
        (df['lon'] <= KENYA_UGANDA_BOUNDS["max_lon"]) &
        (df['lat'] >= KENYA_UGANDA_BOUNDS["min_lat"]) &
        (df['lat'] <= KENYA_UGANDA_BOUNDS["max_lat"])
    ]

    processed_df = pd.DataFrame()
    processed_df['lat'] = df['lat']
    processed_df['lon'] = df['lon']
    processed_df['shop_name'] = df['name']
    processed_df['shop_type'] = "SSC"

    return processed_df

@st.cache_data
def load_csv(url: str) -> pd.DataFrame:
    """Load CSV with error handling."""
    try:
        return pd.read_csv(url)
    except Exception as e:
        st.error(f"Error loading data from {url}: {str(e)}")
        return pd.DataFrame()

def create_circle_boundaries(center_lat: float, center_lon: float, radius_km: float = 10) -> List[List[float]]:
    """Create circle coordinates around a center point."""
    circle_points = []
    for angle in range(0, 360, 10):  # Create points every 10 degrees
        # Convert degrees to radians
        angle_rad = np.radians(angle)
        
        # Calculate point on circle (approximate conversion: 1° ≈ 111 km)
        lat_offset = (radius_km / 111) * np.cos(angle_rad)
        lon_offset = (radius_km / (111 * np.cos(np.radians(center_lat)))) * np.sin(angle_rad)
        
        circle_points.append([center_lon + lon_offset, center_lat + lat_offset])
    
    # Close the circle
    circle_points.append(circle_points[0])
    return circle_points

def create_convex_hull(points: List[List[float]]) -> Optional[List[List[float]]]:
    """Create a convex hull around a set of points."""
    if len(points) < 3:
        return None
    
    try:
        points_array = np.array(points)
        hull = ConvexHull(points_array)
        hull_points = points_array[hull.vertices].tolist()
        # Close the polygon
        hull_points.append(hull_points[0])
        return hull_points
    except:
        return None

def create_map_layers(data: pd.DataFrame, color_map: Dict, show_boundaries: bool, boundary_type: str, boundary_radius: float) -> List[pdk.Layer]:
    """Create map layers with optional boundaries."""
    layers = []
    
    # Create scatter plot layer for points
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position=["lon", "lat"],
        get_color="color",
        get_radius=600,
        pickable=True,
    )
    layers.append(scatter_layer)
    
    # Add boundaries if enabled
    if show_boundaries:
        if boundary_type == "Individual Circles":
            # Create circles around each agro-dealer
            agro_dealers = data[data["shop_type"] == "Agro-Dealer"]
            for _, dealer in agro_dealers.iterrows():
                circle_points = create_circle_boundaries(dealer['lat'], dealer['lon'], boundary_radius)
                
                boundary_layer = pdk.Layer(
                    "PolygonLayer",
                    data=[{
                        "polygon": circle_points,
                        "dealer_name": dealer['shop_name']
                    }],
                    get_polygon="polygon",
                    get_fill_color=[255, 200, 0, 50],  # Semi-transparent yellow
                    get_line_color=[255, 150, 0, 200],  # Orange border
                    get_line_width=50,
                    pickable=True,
                )
                layers.append(boundary_layer)
        
        elif boundary_type == "Dealer Coverage Areas":
            # Create convex hulls for each agro-dealer's customers
            agro_dealers = data[data["shop_type"] == "Agro-Dealer"]
            customers = data[data["shop_type"] == "Customer"]
            
            for _, dealer in agro_dealers.iterrows():
                # Find customers within radius (approximate)
                dealer_point = np.array([dealer['lon'], dealer['lat']])
                customer_points = []
                
                for _, customer in customers.iterrows():
                    customer_point = np.array([customer['lon'], customer['lat']])
                    distance = np.linalg.norm(dealer_point - customer_point) * 111  # Approx km
                    if distance <= boundary_radius:
                        customer_points.append([customer['lon'], customer['lat']])
                
                if len(customer_points) >= 3:
                    hull_points = create_convex_hull(customer_points)
                    if hull_points:
                        boundary_layer = pdk.Layer(
                            "PolygonLayer",
                            data=[{
                                "polygon": hull_points,
                                "dealer_name": dealer['shop_name'],
                                "customer_count": len(customer_points)
                            }],
                            get_polygon="polygon",
                            get_fill_color=[0, 200, 255, 50],  # Semi-transparent blue
                            get_line_color=[0, 150, 255, 200],  # Blue border
                            get_line_width=50,
                            pickable=True,
                        )
                        layers.append(boundary_layer)
    
    return layers

def create_cluster_layer(data: pd.DataFrame) -> pdk.Layer:
    """Create a cluster layer for better visualization of dense areas."""
    return pdk.Layer(
        "HexagonLayer",
        data=data,
        get_position=["lon", "lat"],
        radius=100,
        elevation_scale=4,
        elevation_range=[0, 1000],
        extruded=True,
        coverage=1,
    )

def get_kenya_uganda_view_state(data: pd.DataFrame) -> pdk.ViewState:
    """Get view state focused on Kenya and Uganda."""
    # Default center coordinates for Kenya/Uganda region
    default_lat = 0.5  # Rough center between Kenya and Uganda
    default_lon = 35.0  # Rough center between Kenya and Uganda
    
    if not data.empty:
        lat = data["lat"].mean()
        lon = data["lon"].mean()
        
        # Constrain the view to Kenya/Uganda bounds
        lat = max(min(lat, KENYA_UGANDA_BOUNDS["max_lat"] - 2), KENYA_UGANDA_BOUNDS["min_lat"] + 2)
        lon = max(min(lon, KENYA_UGANDA_BOUNDS["max_lon"] - 2), KENYA_UGANDA_BOUNDS["min_lon"] + 2)
        
        return pdk.ViewState(
            latitude=lat,
            longitude=lon,
            zoom=6,
            pitch=0,
        )
    else:
        return pdk.ViewState(
            latitude=default_lat,
            longitude=default_lon,
            zoom=6,
            pitch=0,
        )

def main():
    st.set_page_config(page_title="Kenya & Uganda Location Visualizer", page_icon="🌍", layout="wide")
    
    # Load CSS for styling
    st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .info-box {
        padding: 20px;
        background-color: #f0f2f6;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    .region-badge {
        background-color: #ff4b4b;
        color: white;
        padding: 5px 10px;
        border-radius: 15px;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<h1 class="main-header">🌍 Kenya & Uganda Location Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<div class="region-badge">Focus: Kenya & Uganda Region</div>', unsafe_allow_html=True)
    
    # File paths
    agro_dealers_file_path = "https://raw.githubusercontent.com/SunCulture/sunculture-data/refs/heads/rodgers-dev/data-sets/all_shops_02202025.csv"
    customers_file_path = "https://raw.githubusercontent.com/SunCulture/sunculture-data/refs/heads/rodgers-dev/data-sets/customers.csv"
    sscs_file_url = 'https://raw.githubusercontent.com/SunCulture/sunculture-data/refs/heads/rodgers-dev/data-sets/sscs.csv'
    
    # Info box
    with st.expander("ℹ️ About this dashboard"):
        st.markdown("""
        This dashboard visualizes locations specifically in **Kenya and Uganda**:
        - **Agro-Dealers**: Agricultural product retailers
        - **Customers**: SunCulture customers
        - **SSCs**: Service and Support Centers
        
        *Note: Only locations within Kenya and Uganda are shown.*
        """)
    
    # Load data
    with st.spinner('Loading Kenya & Uganda data...'):
        agro_dealers = load_csv(agro_dealers_file_path)
        customers = load_csv(customers_file_path)
        sscs = load_csv(sscs_file_url)
    
    # Check if data loaded successfully
    if agro_dealers.empty or customers.empty or sscs.empty:
        st.error("Failed to load one or more datasets. Please check the data sources.")
        return
    
    # Debug: Show data types to understand the structure
    with st.expander("Data Debug Info"):
        st.write("Agro-Dealers columns:", agro_dealers.columns.tolist())
        st.write("Customers columns:", customers.columns.tolist())
        st.write("SSCs columns:", sscs.columns.tolist())
        
        if 'latitude' in customers.columns:
            st.write("Customers latitude sample:", customers['latitude'].head())
            st.write("Customers latitude dtype:", customers['latitude'].dtype)
        if 'longitude' in customers.columns:
            st.write("Customers longitude sample:", customers['longitude'].head())
            st.write("Customers longitude dtype:", customers['longitude'].dtype)
    
    # Process location data
    agro_dealers_data = preprocess_agro_dealers_data(agro_dealers)
    customers_data = preprocess_customers_data(customers)
    ssc_data = preprocess_ssc_data(sscs)
    
    # Combine datasets
    map_data = pd.concat([agro_dealers_data, customers_data, ssc_data], ignore_index=True)
    map_data["lat"] = pd.to_numeric(map_data["lat"], errors="coerce")
    map_data["lon"] = pd.to_numeric(map_data["lon"], errors="coerce")
    
    # Display region statistics
    col_stats1, col_stats2, col_stats3 = st.columns(3)
    with col_stats1:
        st.metric("Agro-Dealers", len(agro_dealers_data))
    with col_stats2:
        st.metric("Customers", len(customers_data))
    with col_stats3:
        st.metric("SSCs", len(ssc_data))
    
    # Create two columns for layout
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.header("Filters & Controls")
        
        # Shop Type Filter
        shop_type_options = ["All", "Agro-Dealer", "Customer", "SSC"]
        selected_shop_types = st.multiselect(
            "Select Shop Type", 
            shop_type_options, 
            default="All"
        )
        
        # Shop Name Filter
        shop_name_options = ["All"] + sorted(map_data["shop_name"].unique().tolist())
        selected_shops = st.multiselect(
            "Select Shop(s)", 
            shop_name_options, 
            default="All"
        )
        
        # Boundary controls
        st.subheader("Boundary Settings")
        show_boundaries = st.checkbox("Show Boundaries", value=True)
        
        if show_boundaries:
            boundary_type = st.radio(
                "Boundary Type",
                ["Individual Circles", "Dealer Coverage Areas"],
                help="Circles: Fixed radius around dealers. Coverage Areas: Convex hull around customers near each dealer."
            )
            
            boundary_radius = st.slider(
                "Boundary Radius (km)",
                min_value=1,
                max_value=50,
                value=10,
                step=1
            )
        else:
            boundary_type = "Individual Circles"
            boundary_radius = 10
        
        # Map style selector
        map_style = st.selectbox(
            "Map Style",
            ["light", "dark", "satellite", "road", "outdoor"]
        )
        
        # View type selector
        view_type = st.radio(
            "View Type",
            ["Standard", "Clustered"],
            horizontal=True
        )
        
        # Radius selector
        radius = st.slider(
            "Marker Size",
            min_value=100,
            max_value=1000,
            value=600,
            step=100
        )
    
    # Apply filters
    filtered_data = map_data
    if "All" not in selected_shop_types:
        filtered_data = filtered_data[filtered_data["shop_type"].isin(selected_shop_types)]
    
    if "All" not in selected_shops:
        filtered_data = filtered_data[filtered_data["shop_name"].isin(selected_shops)]
    
    # Define color mapping
    color_map = {
        "Agro-Dealer": [255, 0, 0, 200],    # Red for Agro-Dealers
        "Customer": [0, 0, 255, 200],       # Blue for Customers
        "SSC": [0, 255, 0, 200]             # Green for SSCs
    }
    filtered_data["color"] = filtered_data["shop_type"].map(color_map)
    
    with col2:
        st.header("Kenya & Uganda Location Visualization")
        
        if filtered_data.empty:
            st.warning("No data available with the current filters.")
        else:
            # Get view state focused on Kenya/Uganda
            view_state = get_kenya_uganda_view_state(filtered_data)
            
            # Create layers
            layers = create_map_layers(filtered_data, color_map, show_boundaries, boundary_type, boundary_radius)
            
            if view_type == "Clustered":
                layers.append(create_cluster_layer(filtered_data))
            
            # Render the map with constrained bounds
            st.pydeck_chart(pdk.Deck(
                layers=layers,
                initial_view_state=view_state,
                tooltip={"text": "{shop_name} ({shop_type})"},
                map_style=map_style,
            ))
            
            # Display stats
            st.metric("Filtered Locations", len(filtered_data))
            
            # Show boundary legend if boundaries are enabled
            if show_boundaries:
                st.info(f"**Boundaries shown**: {boundary_type} with {boundary_radius}km radius")
            
            # Show filtered data table
            with st.expander("View Filtered Data"):
                st.dataframe(
                    filtered_data[['shop_name', 'shop_type', 'lat', 'lon']],
                    use_container_width=True
                )
            
            # Download button
            csv = filtered_data.to_csv(index=False)
            st.download_button(
                label="Download filtered data as CSV",
                data=csv,
                file_name="kenya_uganda_locations.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()