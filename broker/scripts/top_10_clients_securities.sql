CREATE OR REPLACE VIEW data_mart_der.top_10_clients_securities AS
	WITH sec_tran_top AS (
		SELECT 
			c.client_id
			, c.client_name
			, st.transaction_type
			, ROW_NUMBER() OVER (
				PARTITION BY st.transaction_type
				ORDER BY
					SUM(
						st.quantity * 
						CASE
							WHEN st.currency = 'USD' THEN st.price * cur.value
							ELSE st.price
						END
					) DESC
			) AS top
			, ROUND(
				CAST(
					SUM(
						st.quantity *
						CASE
							WHEN st.currency = 'USD' THEN st.price * cur.value
							ELSE st.price
						END
					) AS NUMERIC
				), 3
			) AS securities_portfolio
			, MAX(cur.current_date) AS date
		FROM r_dekhtiarev.clients c 
		JOIN r_dekhtiarev.security_transactions st 
			ON c.client_id = st.client_id
		JOIN r_dekhtiarev.currency cur
			ON st.currency = cur.char_code
			AND cur.current_date = (
				SELECT MAX(cur2.current_date)
				FROM r_dekhtiarev.currency cur2
			)
		GROUP BY c.client_id, c.client_name, st.transaction_type
	)
	SELECT *
	FROM sec_tran_top
	WHERE top <= 10
	ORDER BY transaction_type;