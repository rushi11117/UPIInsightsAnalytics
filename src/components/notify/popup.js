import React from 'react';

const Popup = ({ values, onClose }) => {
  return (
    <div className="popup">
      <div className="popup-content">
        <button onClick={onClose}>Close</button>
        <ul>
          {values}
          {/* {values.map((value, index) => (
            <li key={index}>{value}</li>
          ))} */}
        </ul>
      </div>
    </div>
  );
};

export default Popup;
