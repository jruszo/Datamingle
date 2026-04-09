CREATE DATABASE IF NOT EXISTS demo_orders;
CREATE DATABASE IF NOT EXISTS demo_billing;

GRANT ALL PRIVILEGES ON demo_orders.* TO 'demo_archery'@'%';
GRANT ALL PRIVILEGES ON demo_billing.* TO 'demo_archery'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'demo_archery'@'%';
FLUSH PRIVILEGES;

USE demo_orders;

CREATE TABLE IF NOT EXISTS customers (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  full_name VARCHAR(255) NOT NULL,
  lifecycle_state VARCHAR(32) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  customer_email VARCHAR(255) NOT NULL,
  total_amount DECIMAL(10, 2) NOT NULL,
  order_status VARCHAR(32) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (email, full_name, lifecycle_state)
VALUES
  ('ava@example.com', 'Ava Carter', 'active'),
  ('noah@example.com', 'Noah Hughes', 'trial')
ON DUPLICATE KEY UPDATE
  full_name = VALUES(full_name),
  lifecycle_state = VALUES(lifecycle_state);

INSERT INTO orders (customer_email, total_amount, order_status)
VALUES
  ('ava@example.com', 42.50, 'paid'),
  ('noah@example.com', 13.75, 'pending');

USE demo_billing;

CREATE TABLE IF NOT EXISTS invoices (
  id INT PRIMARY KEY AUTO_INCREMENT,
  invoice_number VARCHAR(64) NOT NULL UNIQUE,
  invoice_status VARCHAR(32) NOT NULL,
  amount_due DECIMAL(10, 2) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO invoices (invoice_number, invoice_status, amount_due)
VALUES
  ('INV-1001', 'open', 99.99),
  ('INV-1002', 'paid', 49.50)
ON DUPLICATE KEY UPDATE
  invoice_status = VALUES(invoice_status),
  amount_due = VALUES(amount_due);
