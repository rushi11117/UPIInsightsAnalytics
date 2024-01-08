import HeaderComponent from './components/header';
import Footer from './components/footer';
import Sidebar from './components/sidebar';
import HomeComponent from './components/homeComponent';
import ReportComponent from './components/resource/reportComponent';
import DynamicTable from './components/dynamicTable';
import ChartComponent from './components/chartComponent';
import Map from './components/map'
import 'bootstrap/dist/css/bootstrap.min.css';
import { BrowserRouter, Route, Routes } from 'react-router-dom';


function App() {
  return (

    <BrowserRouter >
      <div className="App">
        <div>
          <div className='sticky-top'>
            <HeaderComponent />
          </div>
          <div className="container-fluid">
            <div className="row mt-4 bg-light">
              <Sidebar />
              <Routes>
                <Route path='/transactions' exact element={<ChartComponent />} />
                <Route path='/home' exact element={<HomeComponent />} />
                <Route path='/reports' exact element={<ReportComponent />} />
                <Route path='/dynamic' exact element={<DynamicTable />} />
                <Route path='/map' exact element={<Map />} />
              </Routes>
              {/* <ChartComponent /> */}
            </div>
          </div>

          <div>
            <Footer />
          </div>
        </div>
      </div>
    </BrowserRouter >
  );
}

export default App;