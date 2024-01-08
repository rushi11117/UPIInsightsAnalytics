import React from 'react'
import Logoicon from './resource/logo192.ico'
import Logotext from './resource/logo'
import { Link } from 'react-router-dom';

const HeaderComponent = () => {

    // eslint-disable-next-line react-hooks/rules-of-hooks
    // const [expanded, setExpanded] = useState(false);

    // const handleNavToggle = () => {
    //     setExpanded(!expanded)
    // }



    const columnStyle = {
        display: 'flex',
        flexDirection: 'column',
        height: '32px',
        alignItems: 'center',
        justifyContent: 'space-between',
    };

    const imgStyle = {
        height: '25px',
        width: '25px',
    };

    const header = {
        height: '35px',
    }



    return (
        <div style={header}>
            <header className="navbar navbar-expand-lg navbar-light bg-light sticky-top border-bottom" >
                <div className='navbar-nav navbar-brand'>
                <Link to="/home" >
                    <div className="column m-2" style={columnStyle}>
                        <div className='row'>
                            <div className='col-2'>
                                <img src={Logoicon} alt="Logo" style={imgStyle} className='m-1' />
                            </div>
                            <div className='col-9 '>
                                <Logotext />
                            </div>
                        </div>
                    </div>
                </Link>
                </div>
                <div className='navbar-nav'>
                    <button className="navbar-toggler position-absolute d-md-none collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#sidebarMenu" aria-controls="sidebarMenu" aria-expanded="false" aria-label="Toggle navigation">
                        <span className="navbar-toggler-icon"></span>
                    </button>
                </div>

                <div className='w-100 nabar-nav'>
                    <input className="form-control form-control-dark  " type="text" placeholder="Search" aria-label="Search" />
                </div>

                <div className="navbar-nav">
                    <div className="nav-item text-nowrap">
                        <Link to="#" className="nav-link  px-3" >
                            <button type="button" className="btn btn-sm btn-outline-secondary">
                                Sign out
                            </button>
                        </Link>
                    </div>
                </div>
            </header>
        </div >
    )
}

export default HeaderComponent;