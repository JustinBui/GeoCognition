import { useNavigate } from 'react-router-dom';
import MapView from './MapView';

function ExplorePage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen bg-black flex flex-col items-center gap-3 sm:gap-6 px-4 sm:px-6 pt-6 pb-8">
      <h1 className="text-3xl sm:text-7xl font-bold tracking-tight text-lime-400">Explore Data</h1>

      <div className="flex items-center gap-2">
        <div className="h-px w-16 bg-lime-400/40" />
        <div className="h-1.5 w-1.5 rounded-full bg-lime-400" />
        <div className="h-px w-16 bg-lime-400/40" />
      </div>
      <button
        onClick={() => navigate('/')}
        className="px-5 py-2 rounded-md bg-lime-400 text-black text-sm font-semibold hover:bg-lime-300 transition-colors"
      >
        Back
      </button>
      <div className="w-full">
        <MapView />
      </div>
    </div>
  );
}

export default ExplorePage;
