import React, { useEffect, useState } from 'react';
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet';
import shp from 'shpjs';

const ShapefileViewer = () => {
  const [geojson, setGeojson] = useState(null);

  useEffect(() => {
    const loadShapefile = async () => {
      try {
        const shapefileUrl = 'D:\\Data Visualization\\shapefiles\\maharashtra_district.shp';
        const geojsonData = await shp(shapefileUrl);
        setGeojson(geojsonData);
      } catch (error) {
        console.error('Error loading Shapefile:', error);
      }
    };

    loadShapefile();
  }, []);

  return (
    <div className='col-md-9 ms-sm-auto col-lg-10 px-md-4'>
      <h1>Shapefile Viewer</h1>
      {geojson && (
        <MapContainer style={{ height: '500px', width: '100%' }} center={[20.5937, 78.9629]} zoom={6}>
          <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
          <GeoJSON data={geojson} />
        </MapContainer>
      )}
    </div>
  );
};

export default ShapefileViewer;
