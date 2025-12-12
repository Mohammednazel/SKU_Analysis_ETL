-- step6_db/insert_helpers.sql

-- Helper to parse ISO / Date conversions can be done in Python, expecting order_date/cdate as timestamptz strings.

CREATE OR REPLACE FUNCTION app_core.insert_header_if_not_exists(_payload jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _po text := _payload->>'purchase_order_id';
    _order_date timestamptz := (_payload->>'order_date')::timestamptz;
BEGIN
    INSERT INTO app_core.purchase_order_headers (
        purchase_order_id, order_date, buyer_company_name, buyer_email,
        supplier_company_name, supplier_id, subtotal, tax, grand_amount,
        currency, status, cdate, _raw_json
    ) VALUES (
        _po,
        _order_date,
        _payload->>'buyer_company_name',
        _payload->>'buyer_email',
        _payload->>'supplier_company_name',
        _payload->>'supplier_id',
        NULLIF(_payload->>'Subtotal','')::numeric,
        NULLIF(_payload->>'tax','')::numeric,
        NULLIF(_payload->>'grand_amount','')::numeric,
        _payload->>'currency',
        _payload->>'status',
        (_payload->>'cdate')::timestamptz,
        _payload
    )
    ON CONFLICT (purchase_order_id, order_date) DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION app_core.insert_item_with_audit(_payload jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    p_id text := _payload->>'purchase_order_id';
    p_no text := _payload->>'purchase_order_no';
    o_date timestamptz := (_payload->>'order_date')::timestamptz;
    q numeric := NULLIF(_payload->>'quantity','')::numeric;
    up numeric := NULLIF(_payload->>'unit_price','')::numeric;
    tot numeric := NULLIF(_payload->>'total','')::numeric;
    cur text := _payload->>'currency';
    existing jsonb;
    diffs text[] := ARRAY[]::text[];
BEGIN
    -- Try insert
    BEGIN
        INSERT INTO app_core.purchase_order_items (
            purchase_order_id, purchase_order_no, item_id, description,
            quantity, unit_of_measure, unit_price, total, currency,
            order_date, cdate, supplier_id, plant, material_group, product_id, _raw_json
        )
        VALUES (
            p_id, p_no, _payload->>'item_id', _payload->>'description',
            q, _payload->>'unit_of_measure', up, tot, cur,
            o_date, (_payload->>'cdate')::timestamptz, _payload->>'supplier_id',
            _payload->>'plant', _payload->>'material_group', _payload->>'product_id', _payload
        );
        RETURN;
    EXCEPTION WHEN unique_violation THEN
        -- On conflict, fetch existing row
        SELECT row_to_json(t)::jsonb INTO existing
        FROM app_core.purchase_order_items t
        WHERE t.purchase_order_id = p_id AND t.purchase_order_no = p_no
        LIMIT 1;

        -- Compare important fields: quantity, unit_price, total, item_id, description
        IF existing->>'quantity' IS DISTINCT FROM _payload->>'quantity' THEN diffs := array_append(diffs, 'quantity'); END IF;
        IF existing->>'unit_price' IS DISTINCT FROM _payload->>'unit_price' THEN diffs := array_append(diffs, 'unit_price'); END IF;
        IF existing->>'total' IS DISTINCT FROM _payload->>'total' THEN diffs := array_append(diffs, 'total'); END IF;
        IF existing->>'item_id' IS DISTINCT FROM _payload->>'item_id' THEN diffs := array_append(diffs, 'item_id'); END IF;
        IF existing->>'description' IS DISTINCT FROM _payload->>'description' THEN diffs := array_append(diffs, 'description'); END IF;

        IF array_length(diffs,1) IS NULL THEN
            -- No difference -> identical duplicate; do nothing (no audit)
            RETURN;
        ELSE
            -- Insert audit row with existing + incoming
            INSERT INTO app_core.audit_conflicts (table_name, pk, existing_row, incoming_row, diff_fields)
            VALUES ('purchase_order_items', jsonb_build_object('purchase_order_id', p_id, 'purchase_order_no', p_no), existing, _payload, diffs);
            RETURN;
        END IF;
    END;
END;
$$;
