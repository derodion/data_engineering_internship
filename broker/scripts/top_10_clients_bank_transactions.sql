CREATE OR REPLACE VIEW data_mart_der.top_10_clients_bank_transactions AS
	WITH transaction_sums_top AS (
	    SELECT 
	        c.client_id
	        , c.client_name
	        , bt.transaction_type
	        , ROW_NUMBER() OVER (
	            PARTITION BY bt.transaction_type 
	            ORDER BY 
	                SUM(
	                    CASE 
	                        WHEN bt.currency = 'USD' THEN bt.amount * cur.value
	                        ELSE bt.amount
	                    END
	                ) DESC
	        ) AS top
	        , ROUND(
	            CAST(
	                SUM(
	                    CASE 
	                        WHEN bt.currency = 'USD' THEN bt.amount * cur.value
	                        ELSE bt.amount
	                    END
	                ) AS NUMERIC
	            ), 3
	        ) AS rounded_total_amount
	        , MAX(cur.current_date) AS date
	    FROM r_dekhtiarev.clients c
	    JOIN r_dekhtiarev.bank_transactions bt 
	        ON c.client_id = bt.client_id
	    JOIN r_dekhtiarev.currency cur
	        ON bt.currency = cur.char_code
			AND cur.current_date = (
				SELECT MAX(cur2.current_date)
				FROM r_dekhtiarev.currency cur2
			)
	    GROUP BY c.client_id, c.client_name, bt.transaction_type
	)
	SELECT *
	FROM transaction_sums_top
	WHERE top <= 10
	ORDER BY transaction_type;