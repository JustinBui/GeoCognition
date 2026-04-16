import { BrowserRouter, Routes, Route } from 'react-router-dom';
import ExplorePage from './components/ExplorePage';
import Home from './components/Home';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/explore" element={<ExplorePage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
