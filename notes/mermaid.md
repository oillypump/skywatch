graph LR
subgraph "EXTERNAL"
API["<b>Weather & AQI API</b>"]
end

    subgraph "BRONZE LAYER (Raw)"
        direction TB
        B1["raw_aqi_index"]
        B2["raw_weather_forecast"]
    end

    subgraph "SILVER LAYER (Cleansed)"
        direction TB
        S1["aqi_index"]
        S2["forecast_weather"]
    end

    subgraph "GOLD LAYER (Analytics)"
        direction TB
        G1["<b>fact_aqi_weather</b>"]
        D1["dim_city"]
        D2["dim_aqi"]
    end

    %% Connections
    API ==>|"Python Scraper"| B1 & B2

    B1 -.->|"dbt incremental"| S1
    B2 -.->|"dbt incremental"| S2

    S1 & S2 ==>|"dbt join & transform"| G1
    D1 & D2 -.->|"Lookup"| G1

    %% Styling - Color Palette
    classDef external fill:#f8f9fa,stroke:#343a40,stroke-width:2px,color:#343a40;
    classDef bronze fill:#cd7f32,stroke:#3e2723,color:#fff,stroke-width:2px;
    classDef silver fill:#bdc3c7,stroke:#2c3e50,color:#2c3e50,stroke-width:2px;
    classDef gold fill:#f1c40f,stroke:#9a7d0a,color:#3e2723,stroke-width:3px;
    classDef dim fill:#fef9e7,stroke:#9a7d0a,color:#9a7d0a,stroke-dasharray: 5 5;

    %% Assign Classes
    class API external;
    class B1,B2 bronze;
    class S1,S2 silver;
    class G1 gold;
    class D1,D2 dim;
