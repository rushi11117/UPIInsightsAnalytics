import React from 'react';

const Logo = () => {
  const upiOfflineStyle = {
    color: '#ed752e',
    fontWeight: 'bold',
    fontSize: '14px',
    display: 'inline-block',
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    verticalAlign: 'middle',
    paddingRight: '4px',
    paddingLeft: '4px'
  };

  const insightsStyle = {
    color: '#097939',
    fontWeight: 'bold',
    fontSize: '14px',
    display: 'inline-block',
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    verticalAlign: 'middle',
  };

  return (
    <div>
      <span style={upiOfflineStyle}>UPIOffline</span>
      <span style={insightsStyle}> Insights</span>
    </div>
  );
};

export default Logo;
