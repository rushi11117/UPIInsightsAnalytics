import React from 'react';

const Stats = ({ stat }) => {
  return (
    <div className='container'>
      <div className='row border-bottom m-4'>
        <div className='col'>
          <h4>{stat.key}</h4>
        </div>
        <div className='col'>
          <h4>
            {stat.currency ? `${stat.currency} ${stat.value}` : stat.value}
          </h4>
        </div>
      </div>
    </div>
  );
};

export default Stats;
