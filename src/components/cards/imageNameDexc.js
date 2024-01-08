import React from 'react';
import { Col, Card } from 'react-bootstrap';

const ImageNameDexc = ({ dataObject }) => {

    const handleMouseEnter = (e) => {
        e.target.style.transform = 'scale(1.1)';
        e.target.style.transition = 'transform 0.3s ease';
    };

    const handleMouseLeave = (e) => {
        e.target.style.transform = 'scale(1)';
    };
    return (
        <div>
            {dataObject && dataObject.length > 0 && (
                <div className="row row-cols-1 row-cols-md-2 row-cols-lg-4 g-4">
                    {dataObject.map(({ id, name, image, expand }) => (
                        <Col key={id} xs={6} sm={4} md={3} lg={2} className="mb-3">
                            <a href={expand} className=' text-decoration-none ' target="_blank" rel="noopener noreferrer">
                                <Card
                                    className="dataObject-card "
                                    onMouseEnter={handleMouseEnter}
                                    onMouseLeave={handleMouseLeave}
                                >
                                    <Card.Body className="text-center">
                                        <img
                                            className="dataObject-logo img-fluid"
                                            src={image}
                                            alt={name}
                                            width="100"
                                            height="100"
                                        />
                                        <div className="mt-2" style={{ fontSize: '20px' }}>
                                            {name}
                                        </div>
                                    </Card.Body>
                                </Card>
                            </a>
                        </Col>
                    ))}
                </div>
            )}
        </div>
    );
};

export default ImageNameDexc;
