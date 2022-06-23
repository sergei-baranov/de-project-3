ALTER TABLE staging.user_order_log
ADD COLUMN "status" varchar(20) default 'shipped';

ALTER TABLE staging.user_order_log
ADD COLUMN quantity_signed bigint GENERATED ALWAYS AS
(CASE WHEN "status" = 'refunded' THEN quantity * -1 ELSE quantity END)
STORED;

ALTER TABLE staging.user_order_log
ADD COLUMN payment_amount_signed numeric(10,2) GENERATED ALWAYS AS
(CASE WHEN "status" = 'refunded' THEN payment_amount * -1 ELSE payment_amount END)
STORED;