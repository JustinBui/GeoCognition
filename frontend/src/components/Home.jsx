import { useNavigate } from 'react-router-dom';

function Home() {
  const navigate = useNavigate();
  return (
    <div className="min-h-screen bg-black flex flex-col items-center justify-center">
      {/* Subtle grid background */}
      <div className="absolute inset-0 bg-[linear-gradient(rgba(163,230,53,0.03)_1px,transparent_1px),linear-gradient(90deg,rgba(163,230,53,0.03)_1px,transparent_1px)] bg-[size:40px_40px]" />

      <div className="relative z-10 flex flex-col items-center gap-6 text-center px-6">
        {/* Tag line above title */}
        <span className="text-xs font-mono tracking-[0.3em] uppercase text-lime-400/70">
          Earth Data Analytics Platform
        </span>

        {/* Main title */}
        <h1 className="text-4xl sm:text-7xl font-bold tracking-tight">
          <span className="text-white">Geo</span>
          <span className="text-lime-400">Cognition</span>
        </h1>

        {/* Accent underline */}
        <div className="flex items-center gap-2">
          <div className="h-px w-16 bg-lime-400/40" />
          <div className="h-1.5 w-1.5 rounded-full bg-lime-400" />
          <div className="h-px w-16 bg-lime-400/40" />
        </div>

        {/* Subtitle */}
        <p className="max-w-md text-sm text-zinc-400 leading-relaxed">
          Real-time platform for spatial analytics, powered by modern data infrastructure.
        </p>

        {/* CTA buttons */}
        <div className="flex gap-4 mt-4">
          <button
            onClick={() => navigate('/explore')}
            className="px-6 py-2.5 rounded-md bg-lime-400 text-black text-sm font-semibold hover:bg-lime-300 transition-colors"
          >
            Explore Data
          </button>
          {/* <button className="px-6 py-2.5 rounded-md border border-lime-500/50 text-lime-400 text-sm font-semibold hover:bg-lime-500/10 transition-colors">
            API Docs
          </button> */}
        </div>

        {/* Status indicator */}
        <div className="flex items-center gap-2 mt-6 text-xs text-zinc-500 font-mono">
          <span className="inline-block h-2 w-2 rounded-full bg-lime-400 animate-pulse" />
          Pipeline active
        </div>
      </div>
    </div>
  );
}

export default Home;
