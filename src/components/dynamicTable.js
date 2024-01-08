import React, { useState } from 'react';
import moment from 'moment';
import stringProcessing from './Utils/stringProcessing';
import ToastComponent from './notify/toastComponent';

const DynamicTable = ({ data, link }) => {
    const [searchTerm, setSearchTerm] = useState('');
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 14; // Number of items per page

    const isIdPresent = data.length > 0 && data[0].hasOwnProperty('id');

    const filteredData = data.filter(item =>
        isIdPresent && item.id.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const indexOfLastItem = currentPage * itemsPerPage;
    const indexOfFirstItem = indexOfLastItem - itemsPerPage;
    const currentItems = filteredData.slice(indexOfFirstItem, indexOfLastItem);

    const paginate = pageNumber => setCurrentPage(pageNumber);

    const [showModal, setShowModal] = useState(false);
    const [selectedTransaction, setSelectedTransaction] = useState(null);

    const handleShowModal = (transaction) => {
        setSelectedTransaction(transaction);
        setShowModal(true);
    };

    const handleCloseModal = () => {
        setShowModal(false);
        setSelectedTransaction(null);
    };

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
    };

    return (
        <div className='container'>
            {data.id && (
                <input
                    type="text"
                    placeholder="Search by transaction id"
                    value={searchTerm}
                    onChange={e => setSearchTerm(e.target.value)}
                />
            )}
            <div>Sort By :</div>
            <div className="table-scroll">
                <div className="table-responsive">
                    <table className="table">
                        <thead>
                            <tr>
                                <th scope='col' className='border'> SrNo</th>
                                {Object.keys(data[0]).map((key, index) => (
                                    <th scope='col' key={index} className='border'>
                                        {stringProcessing(key)}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {currentItems.map((transaction, index) => (
                                <tr key={index}>
                                    <td className='border'>
                                        {(currentPage - 1) * itemsPerPage + index + 1}
                                    </td>
                                    <td className='border'>
                                        <a href='#' onClick={() => handleShowModal(transaction)}>
                                            {transaction.id}
                                        </a>
                                        {selectedTransaction && (
                                            <ToastComponent
                                                showModal={showModal}
                                                handleCloseModal={handleCloseModal}
                                                objectsId={selectedTransaction.id}
                                                objects={selectedTransaction}
                                            />
                                        )}
                                    </td>
                                    <td className='border'>{transaction.ammount}</td>
                                    <td className='border'>{moment(transaction.transactionTime).format('MMMM Do YYYY, h:mm:ss A')}</td>
                                    <td className='border'>{moment(transaction.transactionCompletion).format('MMMM Do YYYY, h:mm:ss A')}</td>
                                    <td className='border' style={{ color: getStatusClass(transaction.transactionStatus) }}>
                                        {transaction.transactionStatus}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
            <div className="pagination-scroll">
                <div className="table-responsive">
                    <ul className="pagination">
                        {Array.from({ length: Math.ceil(filteredData.length / itemsPerPage) }, (_, index) => (
                            <li key={index} className={currentPage === index + 1 ? 'page-item active' : 'page-item'}>
                                <button className="page-link" onClick={() => paginate(index + 1)}>
                                    {index + 1}
                                </button>
                            </li>
                        ))}
                    </ul>
                </div>
            </div>
        </div>
    );
};

export default DynamicTable;
