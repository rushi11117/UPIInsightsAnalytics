import React from 'react';
import { Card } from 'react-bootstrap';

const FeatureCard = ({ feature }) => {
    return (
        <Card className=" mb-4 mt-4 ">
            <Card.Body className="text-center">
                {feature && (
                    <>
                        <img
                            className="partner-logo img-fluid"
                            src={feature.image}
                            alt={feature.name}
                        // width="100"
                        // height="100"
                        />
                        <div style={{ fontSize: '20px', margin: '8px 0' }}>
                            {feature.name}
                        </div>
                        <div>{feature.description}</div>
                    </>
                )}
            </Card.Body>
        </Card>
    );
};

export default FeatureCard;
