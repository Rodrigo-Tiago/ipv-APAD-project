-- Fazer alterações aos dados
-- USERS --
INSERT INTO USERS (
    USER_CODE, AGE_GROUP_ID, GENDER_ID, COUNTRY_ID, SUBSCRIPTION_STATUS_ID,
    NAME, EMAIL, SIGNUP_DATE, DISTRICT, CITY, POSTAL_CODE, STREET_ADDRESS
) VALUES (
    'USER_00000000101', 2, 1, 3, 1,
    'João Silva', 'joao.silva@example.com', '2024-05-01',
    'Lisboa', 'Lisboa', '1000-001', 'Rua das Flores, 25'
);

UPDATE USERS
SET
    AGE_GROUP_ID = 3,
    GENDER_ID = 2,
    COUNTRY_ID = 5,
    SUBSCRIPTION_STATUS_ID = 2,
    NAME = 'Updated Name A',
    EMAIL = 'updateda@example.com',
    SIGNUP_DATE = '2024-06-01',
    DISTRICT = 'Porto',
    CITY = 'Porto',
    POSTAL_CODE = '4000-123',
    STREET_ADDRESS = 'Av. da Liberdade, 100'
WHERE USER_CODE = 'USER_00000000001';