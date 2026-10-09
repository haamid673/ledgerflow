-- Fixed-ID system user/account used as the source when funding dev accounts.
INSERT INTO users (id, email, name)
VALUES ('00000000-0000-0000-0000-000000000001', 'system@ledgerflow.internal', 'System');

INSERT INTO accounts (id, user_id, kind, currency)
VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'system', 'INR');
