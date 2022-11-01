CREATE TABLE tb_top_youtubers(
	cod_top_youtubers SERIAL PRIMARY KEY,
	rank INT,
	youtuber VARCHAR(200),
	subscribers INT,
	video_views  VARCHAR(200),
	video_count INT,
	category VARCHAR(200),
	started INT
);


SELECT * FROM tb_top_youtubers;
DO $$
DECLARE
	--1. declaração do cursor
	--esse cursor é "unbound" por ter sido declarado se ser associado a nenhum select
	cur_nomes_youtubers REFCURSOR;
	--para armazenar o nome de cada youtuber a cada iteração
	v_youtuber VARCHAR(200);
BEGIN
	--2. abertura do cursor
	OPEN cur_nomes_youtubers FOR
		SELECT youtuber
		FROM
		tb_top_youtubers;
	LOOP
		--3. Recuperação dos dados de interesse
		FETCH cur_nomes_youtubers INTO v_youtuber;
		EXIT WHEN NOT FOUND;
		RAISE NOTICE '%', v_youtuber;
	END LOOP;
	-- 4. Fechamento do cursor
	CLOSE cur_nomes_youtubers;
END;
$$

DO $$
DECLARE
	--1. Declaração do cursor (unbound)
	cur_nomes_a_partir_de REFCURSOR;
	v_youtuber VARCHAR(200);
	v_ano INT := 2008;
	v_nome_tabela VARCHAR(200) := 'tb_top_youtubers';
BEGIN
	--2. abrir o cursor
	OPEN cur_nomes_a_partir_de FOR EXECUTE
	format(
		'
			SELECT
				youtuber
			FROM 
				%s
			WHERE started >= $1
		',
		v_nome_tabela
	)USING v_ano;
	LOOP
		--3. Recuperação dos dados (FETCH)
		FETCH cur_nomes_a_partir_de INTO v_youtuber;
		EXIT WHEN NOT FOUND;
		RAISE NOTICE '%', v_youtuber;
	END LOOP;
	-- 4. fechamento do cursor
	CLOSE cur_nomes_a_partir_de;
END;
$$

-----------------------------------------
DO $$
DECLARE
	--1. Declaração do cursor
	--cursor (bound ou vinculado)
	cur_nomes_e_inscritos CURSOR FOR SELECT youtuber, subscribers FROM
	tb_top_youtubers;
	tupla RECORD; --tupla é a linha inteira
	-- o operador de acesso a membro é o .
	--tupla.youtuber é o nome do youtuber
	--tupla.subscribers é o número de inscritos
	resultado TEXT DEFAULT '';
BEGIN
	--2. Abrir o cursor
	OPEN cur_nomes_e_inscritos;
	--3. Recuperar dados
	FETCH cur_nomes_e_inscritos INTO tupla;
	WHILE FOUND LOOP
	 resultado := resultado || tupla.youtuber || ': ' || tupla.subscribers || ', ';
	 --3. Recuperar dados
	FETCH cur_nomes_e_inscritos INTO tupla;
	END LOOP;
	--4. Fechamento do cursor
	CLOSE cur_nomes_e_inscritos;
	RAISE NOTICE '%', resultado;
END;
$$

-------------------------------------------------------------------
DO $$
DECLARE
	v_ano INT := 2010;
	v_inscritos INT := 60_000_000;
	v_youtuber VARCHAR(200);
	--1. Declarar o cursor
	--Esse cursor é bound ou vinculado
	cur_ano_inscritos CURSOR (ano INT, inscritos INT) FOR 
		SELECT youtuber 
		FROM tb_top_youtubers WHERE started >= ano AND subscribers >= inscritos;
BEGIN
	--2. Abrir o cursor
	--execute apenas um OPEN
	--versão 1: passando parâmetros pela ordem
	--OPEN cur_ano_inscritos (v_ano, v_inscritos);
	--versão 2: passando parâmetros pelo nome
	OPEN cur_ano_inscritos (inscritos := v_inscritos, ano := v_ano);
	LOOP
		--3. Recuperar dados
		FETCH cur_ano_inscritos INTO v_youtuber;
		EXIT WHEN NOT FOUND;
		RAISE NOTICE '%', v_youtuber;
	END LOOP;
	--4. Fechar cursor
	CLOSE cur_ano_inscritos;
END;
$$
------------------------------------------
DO $$
DECLARE
	--1. Declaração do cursor (esse é unbound ou não vinculado)
	cur_delete REFCURSOR;
	linha RECORD;
BEGIN
	--2. Abertura do cursor
	OPEN cur_delete SCROLL FOR
		SELECT * FROM tb_top_youtubers;
	LOOP
		--3. Recuperação de dados
		FETCH cur_delete INTO linha;
		EXIT WHEN NOT FOUND;
		IF linha.video_count IS NULL THEN
			DELETE FROM tb_top_youtubers WHERE CURRENT OF cur_delete;
		END IF;
	END LOOP;
	LOOP
	 --3. Recuperação de dados (de baixo para cima)
	 FETCH BACKWARD FROM cur_delete INTO linha;
	 EXIT WHEN NOT FOUND;
	 RAISE NOTICE '%', linha;
	END LOOP;
	--4. Fechamento do cursor
	CLOSE cur_delete;
END;
$$

SELECT * FROM tb_top_youtubers;














