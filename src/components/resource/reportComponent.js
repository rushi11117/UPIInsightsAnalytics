import React, { useState } from 'react';
import ImageNameDexc from '../cards/imageNameDexc';
import image from './upi-report-thumbnail.jpg';

const ReportsComponent = () => {
    const [expandedYear, setExpandedYear] = useState(null);

    const currentYear = new Date().getFullYear();
    const currentMonth = new Date().getMonth() + 1; // January is 0, so add 1 to get the current month

    const years = [currentYear, currentYear - 1, currentYear - 2, currentYear - 3];

    const months = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ];

    const toggleExpand = (year) => {
        setExpandedYear(expandedYear === year ? null : year);
    };

    const getMonthsForYear = (year) => {
        const monthsForYear = months.filter((_, idx) => year < currentYear || (year === currentYear && idx < currentMonth));
        return monthsForYear.map((month, idx) => ({ id: idx, name: `${year} ${month}`, image: image }));
    };

    return (
        <div className="col-md-9 ms-sm-auto mt-4 col-lg-10 px-md-4 border-bottom">
            {years.map((year, index) => (
                <div key={index} className="mb-3">
                    <div
                        style={{
                            borderBottom: '1px solid #ccc',
                            marginBottom: '10px',
                            padding: '5px',
                            cursor: 'pointer',
                            display: 'flex',
                            justifyContent: 'space-between'
                        }}
                        onClick={() => toggleExpand(year)}
                    >
                        <span>{year}</span>
                        <span>{expandedYear === year ? '-' : '+'}</span>
                    </div>
                    {expandedYear === year && (
                        <div style={{ marginLeft: '15px' }}>
                            <ImageNameDexc dataObject={getMonthsForYear(year)} />
                        </div>
                    )}
                </div>
            ))}
        </div>
    );
};

export default ReportsComponent;
