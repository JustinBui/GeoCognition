import { useEffect, useRef, useState } from 'react';
import Map from 'ol/Map';
import View from 'ol/View';
import TileLayer from 'ol/layer/Tile';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import Graticule from 'ol/layer/Graticule';
import OSM from 'ol/source/OSM';
import XYZ from 'ol/source/XYZ';
import GeoJSON from 'ol/format/GeoJSON';
import { Style, Stroke, Fill, Text } from 'ol/style';
import { defaults as defaultControls, Rotate } from 'ol/control';
import 'ol/ol.css';

const THEMES = [
  {
    label: 'Standard',
    source: () => new OSM(),
    graticuleColor: 'rgba(0, 220, 255, 0.4)',
  },
  {
    label: 'Dark Matter',
    source: () => new XYZ({ url: 'https://{a-d}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png' }),
    graticuleColor: 'rgba(255, 0, 0, 0.4)',
  },
  {
    label: 'Positron (Light)',
    source: () => new XYZ({ url: 'https://{a-d}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png' }),
    graticuleColor: 'rgba(136, 255, 0, 0.4)',
  },
  {
    label: 'Voyager',
    source: () =>
      new XYZ({ url: 'https://{a-d}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png' }),
    graticuleColor: 'rgba(41, 44, 201, 0.4)',
  },
  {
    label: 'Topographic',
    source: () => new XYZ({ url: 'https://{a-c}.tile.opentopomap.org/{z}/{x}/{y}.png' }),
    graticuleColor: 'rgba(32, 221, 79, 0.4)',
  },
];

const FAULT_GEOJSON_URL =
  'https://raw.githubusercontent.com/fraxen/tectonicplates/master/GeoJSON/PB2002_boundaries.json';

function ToggleButton({ active, onClick, color = 'lime', children }) {
  const activeClasses = {
    orange: 'bg-orange-500/20 border-orange-500 text-orange-400',
    cyan: 'bg-cyan-500/20 border-cyan-500 text-cyan-400',
    lime: 'bg-lime-500/20 border-lime-500 text-lime-400',
  };
  const dotClasses = {
    orange: 'bg-orange-400',
    cyan: 'bg-cyan-400',
    lime: 'bg-lime-400',
  };

  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-2 px-3 py-1.5 rounded-md border text-sm font-semibold transition-colors ${
        active
          ? activeClasses[color]
          : 'bg-black border-lime-400/30 text-lime-400/60 hover:border-lime-400/60 hover:text-lime-400'
      }`}
    >
      <span
        className={`inline-block h-2 w-2 rounded-full ${active ? dotClasses[color] : 'bg-lime-400/40'}`}
      />
      {children}
    </button>
  );
}

function MapView() {
  const mapRef = useRef(null);
  const tileLayerRef = useRef(null);
  const faultLayerRef = useRef(null);
  const graticuleRef = useRef(null);
  const rotateControlRef = useRef(null);
  const mapInstanceRef = useRef(null);

  const [selectedTheme, setSelectedTheme] = useState(0);
  const [showFaults, setShowFaults] = useState(false);
  const [showGraticule, setShowGraticule] = useState(false);

  const buildGraticule = (color, visible) =>
    new Graticule({
      strokeStyle: new Stroke({ color, width: 1 }),
      showLabels: true,
      wrapX: true,
      lonLabelStyle: new Text({
        font: '11px monospace',
        fill: new Fill({ color: '#22d3ee' }),
        stroke: new Stroke({ color: '#000', width: 2 }),
      }),
      latLabelStyle: new Text({
        font: '11px monospace',
        fill: new Fill({ color: '#22d3ee' }),
        stroke: new Stroke({ color: '#000', width: 2 }),
      }),
      visible,
    });

  useEffect(() => {
    const tileLayer = new TileLayer({ source: THEMES[0].source() });
    tileLayerRef.current = tileLayer;

    const faultLayer = new VectorLayer({
      source: new VectorSource({
        url: FAULT_GEOJSON_URL,
        format: new GeoJSON(),
      }),
      style: new Style({
        stroke: new Stroke({ color: '#ff0000', width: 1.5 }),
      }),
      visible: false,
    });
    faultLayerRef.current = faultLayer;

    const graticule = buildGraticule(THEMES[0].graticuleColor, false);
    graticuleRef.current = graticule;

    const rotateControl = new Rotate({ autoHide: false });
    rotateControlRef.current = rotateControl;

    const map = new Map({
      target: mapRef.current,
      layers: [tileLayer, faultLayer, graticule],
      controls: defaultControls({ rotate: false }),
      view: new View({
        center: [0, 0],
        zoom: 2,
        projection: 'EPSG:3857',
      }),
    });

    mapInstanceRef.current = map;

    return () => map.setTarget(null);
  }, []);

  const handleThemeChange = (e) => {
    const idx = Number(e.target.value);
    setSelectedTheme(idx);
    tileLayerRef.current?.setSource(THEMES[idx].source());

    const map = mapInstanceRef.current;
    if (map) {
      map.removeLayer(graticuleRef.current);
      const newGraticule = buildGraticule(THEMES[idx].graticuleColor, showGraticule);
      graticuleRef.current = newGraticule;
      map.addLayer(newGraticule);
    }
  };

  const handleFaultToggle = () => {
    const next = !showFaults;
    setShowFaults(next);
    faultLayerRef.current?.setVisible(next);
  };

  const handleGraticuleToggle = () => {
    const next = !showGraticule;
    setShowGraticule(next);
    graticuleRef.current?.setVisible(next);
  };

  return (
    <div className="w-full flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-xs font-mono tracking-widest uppercase text-lime-400/70">
          Map Theme
        </label>
        <select
          value={selectedTheme}
          onChange={handleThemeChange}
          className="bg-black border border-lime-400/30 text-lime-400 text-sm rounded-md px-3 py-1.5 focus:outline-none focus:border-lime-400 cursor-pointer"
        >
          {THEMES.map((theme, idx) => (
            <option key={idx} value={idx} className="bg-black">
              {theme.label}
            </option>
          ))}
        </select>

        <div className="h-4 w-px bg-lime-400/20" />

        <ToggleButton active={showFaults} onClick={handleFaultToggle} color="orange">
          Fault Lines
        </ToggleButton>

        <ToggleButton active={showGraticule} onClick={handleGraticuleToggle} color="cyan">
          Lat/Lon Grid
        </ToggleButton>
      </div>

      <div
        ref={mapRef}
        className="w-full rounded-lg overflow-hidden border border-lime-400/20"
        style={{ height: 'clamp(350px, 65vh, 800px)' }}
      />
    </div>
  );
}

export default MapView;
