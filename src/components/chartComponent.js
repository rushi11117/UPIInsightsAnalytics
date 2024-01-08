import React, { useEffect, useState } from 'react';
// import { Chart } from 'chart.js/auto';
// import { Bar } from 'react-chartjs-2';
// import userEvent from '@testing-library/user-event';
import DynamicTable from './dynamicTable';
// import 'chartjs-adapter-date-fns';
import Partner from './cards/ourPartners'

import axios from 'axios';
import moment from 'moment';


const ChartComponent = () => {

    const [transactionBufferData, setTransactionBufferData] = useState(null);

    useEffect(() => {
        const fetchTransactionBufferData = async () => {

            axios.get('http://localhost:8085/transactions-insights')
                .then(response => {
                    setTransactionBufferData(response.data);
                    console.log("Data" +JSON.stringify(response))
                })
                .catch(error => {
                    console.log("Error Thrown :", error)
                })
        }
        fetchTransactionBufferData();
    }, [])


    const getStatusClass = (status) => {
        switch (status) {
            case 'TRANSACTION_SUCCESS':
                return 'green';
            case 'TRANSACTION_FAILED':
                return 'red';
            case 'TRANSACTION_PENDING':
                return 'orange';
            default:
                return '';
        }
    }


    const chartOptions = {
        type: 'radar',
        data: transactionBufferData,
        options: {
            datasets: {
                bar: {
                    backgroundColor: 'rgba(255, 99, 132, 0.2)', // Background color for bars
                    borderColor: 'rgba(255, 99, 132, 1)', // Border color for bars
                    borderWidth: 1 // Border width for bars
                }
            },
            // Other configurations...
        }
    };


    return (
        <main className="col-md-9 ms-sm-auto col-lg-10 px-md-4">
            <div className="chartjs-size-monitor">
                <div className="chartjs-size-monitor-expand">
                    <div className="">
                    </div>
                </div>
                <div className="chartjs-size-monitor-shrink">
                    <div className="">
                    </div>
                </div>
            </div>
            <div className="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">
                <h1 className="h2">Dashboard</h1>
                <div className="btn-toolbar mb-2 mb-md-0">
                    <div className="btn-group me-2">
                        <button type="button" className="btn btn-sm btn-outline-secondary">Share</button>
                        <button type="button" className="btn btn-sm btn-outline-secondary">Export</button>
                    </div>
                    <button type="button" className="btn btn-sm btn-outline-secondary dropdown-toggle">
                        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" stroke-linejoin="round" className="feather feather-calendar" aria-hidden="true"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect><line x1="16" y1="2" x2="16" y2="6"></line><line x1="8" y1="2" x2="8" y2="6"></line><line x1="3" y1="10" x2="21" y2="10"></line></svg>
                        This week
                    </button>
                </div>
            </div>
            {/* 
            <div>
                <Bar data={chartData} options={chartOptions} />
            </div> */}

            <div>
                <div className="table-responsive">
                    {transactionBufferData ? (
                        // <InteractiveTable data={transactionBufferData} />
                        <DynamicTable data={transactionBufferData} />
                    ) : (
                        <div>

                        </div>
                    )}
                </div>

                {/* <canvas id="myChart"></canvas> */}
            </div>

            <h4>Section title</h4>
            {/* <div className="table-responsive */}
        </main>
    )
};

export default ChartComponent;
