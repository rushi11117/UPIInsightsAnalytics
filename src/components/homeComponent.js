import React from 'react';
import Partner from './cards/ourPartners';
import transactionGif from './resource/transaction.gif';
import transactionModes from './resource/transaction-modes.gif';
import transactionSecurity from './resource/security.gif'
import FeatureCard from './cards/featureCard';
import Trending from './changing/trending';
import ReportsComponent from './resource/reportComponent';


const HomeComponent = () => {

  const featureArray = [
    { id: 1, name: 'Multiple Transactions Modes', image: transactionModes, discription: "Multiple Mode And Devices Support" },
    { id: 2, name: 'International Transactions', image: transactionGif, discription: "Internaltional Transaction Realtime" },
    { id: 1, name: 'Blockchain Security', image: transactionSecurity, discription: "Internaltional Transaction Realtime" },
  ];

  const traindingStat = [
    { id: 1, key: "Successful Transactions", value: 38973979 },
    { id: 2, key: "Active Users", value: 93979 },
    { id: 3, key: "Daily Transactions", value: 3979, },
    { id: 4, key: "Assets Handled Worth", value: 7673873979, currency: "$" }
  ]

  return (
    <div className="col-md-9 ms-sm-auto col-lg-10 px-md-4">
      <div className='row  row-cols-md-3 g-4 mb-4 mt-4 border-bottom'>
        {
          traindingStat.map((stat) => (
            <div key={stat.key} className='col'>
              <Trending stat={stat} />
            </div>
          ))
        }
      </div>
      <div className="row row-cols-1 row-cols-md-3 g-4 border-bottom ">
        {featureArray.map((feature) => (
          <div key={feature.id} className="col">
            <FeatureCard feature={feature} />
          </div>
        ))}
      </div>
      {/* <div className='row rows-cols-1 mt-4 g-4'>
        <ReportsComponent />
      </div> */}
      <Partner />
    </div>
  );
}

export default HomeComponent;