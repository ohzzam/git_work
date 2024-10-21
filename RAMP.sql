-- �÷�������Ÿ������ sql
SELECT CASE WHEN DATA_TYPE = 'STRING'  THEN 'VARCHAR(' || PREC || ')'  WHEN DATA_TYPE = 'CHAR'  THEN 'CHAR(' || PREC || ')'  WHEN DATA_TYPE = 'NUMERIC' THEN 'NUMERIC(' || PREC || ',' || SCALE || ')' ELSE DATA_TYPE END FROM db_attribute WHERE class_name = 'j_attachfile' AND attr_name = 'dwnld_cnt';

-- alter comment include not null, default
CREATE TABLE imsi_comment (
	class_name varchar(100) NULL ,
	class_name_new varchar(100) NULL ,
	attr_name varchar(100) NULL ,
	attr_name_new varchar(100) NULL ,
	comment1 varchar(100) NULL ,
	comment2 varchar(1000) NULL
);

select distinct alter_txt from 
(SELECT 'ALTER TABLE ' || (select DISTINCT class_name_new from imsi_01 where attr_name = A.attr_name and class_name = A.class_name)
|| ' MODIFY COLUMN ' || (select DISTINCT attr_name_new from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ' ' || 
CASE
WHEN A.data_type = 'STRING'
THEN 'VARCHAR(' || A.prec || ')'
WHEN A.data_type = 'CHAR'
THEN 'CHAR(' || A.prec || ')'
WHEN A.data_type = 'NUMERIC'
THEN 'NUMERIC(' || A.prec || ',' || A.scale || ')'
ELSE A.data_type
end 
|| case when is_nullable = 'NO' then ' NOT NULL ' ELSE ' ' end
|| case when default_value is not null then ' default ' || A.default_value else ' ' end
|| ' COMMENT  ''' || (select DISTINCT comment1 from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ''';' as alter_txt
FROM db_attribute A, db_class B
WHERE A.class_name = B.class_name AND B.class_type = 'CLASS' AND B.is_system_class = 'NO'
and (A.is_nullable = 'NO' or A.default_value is not null)
ORDER BY A.class_name, A.attr_name)
where alter_txt is not null;

--��ü �÷� ����
select count(*) 
FROM db_attribute A, db_class B
WHERE A.class_name = B.class_name AND B.class_type = 'CLASS' AND B.is_system_class = 'NO'
and A.class_name not like 'imsi%';

--���̺�� ���� ������ �������� imsi_table ���̺�� insert
create table imsi_table(class_name varchar(100),class_name_new varchar(100));

--���̺����Ǽ� ��ȸ
SELECT A.CLASS_NAME AS ���̺��_1
         , B.ATTR_NAME  AS �÷�������_2
         , B.COMMENT     AS �ѱ��÷���_3
         , B.IS_NULLABLE     AS NULL����_6
         , B.DATA_TYPE     AS ������Ÿ��_7
         ,  B.PREC  AS  �����ͱ���_8
         ,  B.SCALE  AS  �����ͼҼ�������
         , CASE WHEN D.KEY_ATTR_NAME IS NOT NULL THEN 'Y' END AS PK����_9
         , CASE WHEN F.KEY_ATTR_NAME IS NOT NULL THEN 'Y' END AS  FK����_10 
         , B.DEFAULT_VALUE AS ��������_11
          ,  B.DEF_ORDER + 1 AS �÷�����
  FROM DB_CLASS A
            INNER JOIN DB_ATTRIBUTE B
                ON B.CLASS_NAME = A.CLASS_NAME
            LEFT JOIN DB_INDEX C
                 ON C.IS_PRIMARY_KEY = 'YES'
                AND C.CLASS_NAME = B.CLASS_NAME              
            LEFT JOIN DB_INDEX_KEY D
                 ON  D.CLASS_NAME = C.CLASS_NAME
                AND D.INDEX_NAME = C.INDEX_NAME
                AND D.KEY_ATTR_NAME = B.ATTR_NAME 
            LEFT JOIN DB_INDEX E
                 ON E.IS_FOREIGN_KEY = 'YES'
                AND E.CLASS_NAME = B.CLASS_NAME              
            LEFT JOIN DB_INDEX_KEY F
                 ON  F.CLASS_NAME = E.CLASS_NAME
                AND F.INDEX_NAME = E.INDEX_NAME
                AND F.KEY_ATTR_NAME = B.ATTR_NAME    
 WHERE A.OWNER_NAME = 'RMS_APP'     
ORDER BY B.DEF_ORDER;


======= ojm ======

SELECT * FROM imsi_01 WHERE attr_name_new='prdctn_trdv_cd';
--- new
select distinct alter_txt from 
(SELECT 'ALTER TABLE ' || (select DISTINCT class_name_new from imsi_01 where attr_name = A.attr_name and class_name = A.class_name)
|| ' MODIFY COLUMN ' || (select DISTINCT attr_name_new from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ' ' || 
CASE
WHEN A.data_type = 'STRING'
THEN 'VARCHAR(' || A.prec || ')'
WHEN A.data_type = 'CHAR'
THEN 'CHAR(' || A.prec || ')'
WHEN A.data_type = 'NUMERIC'
THEN 'NUMERIC(' || A.prec || ',' || A.scale || ')'
ELSE A.data_type
end 
|| case when is_nullable = 'NO' then ' NOT NULL ' ELSE ' ' end
|| case when default_value is not null then ' DEFAULT ''' || A.default_value || '''' else ' ' end
|| ' COMMENT  ''' || (select DISTINCT comment1 from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ''';' as alter_txt
FROM db_attribute A, db_class B
WHERE A.class_name = B.class_name AND B.class_type = 'CLASS' AND B.is_system_class = 'NO'
and (A.is_nullable = 'NO' or A.default_value is not null)
ORDER BY A.class_name, A.attr_name)
where alter_txt is not null;



--- old
select distinct alter_txt from 
(SELECT 'ALTER TABLE ' || (select DISTINCT class_name  from imsi_01 where attr_name = A.attr_name and class_name = A.class_name)
|| ' MODIFY COLUMN ' || (select DISTINCT attr_name_new from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ' ' || 
CASE
WHEN A.data_type = 'STRING'
THEN 'VARCHAR(' || A.prec || ')'
WHEN A.data_type = 'CHAR'
THEN 'CHAR(' || A.prec || ')'
WHEN A.data_type = 'NUMERIC'
THEN 'NUMERIC(' || A.prec || ',' || A.scale || ')'
ELSE A.data_type
end 
|| case when is_nullable = 'NO' then ' NOT NULL ' ELSE ' ' end
|| case when default_value is not null then ' DEFAULT ''' || A.default_value || '''' else ' ' end
|| ' COMMENT  ''' || (select DISTINCT comment1 from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ''';' as alter_txt
FROM db_attribute A, db_class B
WHERE A.class_name = B.class_name AND B.class_type = 'CLASS' AND B.is_system_class = 'NO'
--and (A.is_nullable = 'NO' or A.default_value is not null)
ORDER BY A.class_name, A.attr_name)
where alter_txt is not null;

-- RENAME_comumn 

select distinct alter_txt from 
(SELECT 'ALTER TABLE ' || (select DISTINCT class_name  from imsi_01 where attr_name = A.attr_name and class_name = A.class_name)
|| ' RENAME COLUMN ' || (select DISTINCT attr_name from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ' TO ' ||  (select DISTINCT attr_name_new from imsi_01 where attr_name = A.attr_name and class_name = A.class_name) || ';'AS alter_txt --CASE
FROM db_attribute A, db_class B
WHERE A.class_name = B.class_name AND B.class_type = 'CLASS' AND B.is_system_class = 'NO'
ORDER BY A.class_name, A.attr_name)
where alter_txt is not null;



---- rms_com recomment (�ѱ۱���)
SELECT 'ALTER TABLE ' || class_name || ' MODIFY COLUMN ' || attr_name || ' ' ||
CASE
WHEN data_type = 'STRING'
THEN 'VARCHAR(' || prec || ')'
WHEN data_type = 'CHAR'
THEN 'CHAR(' || prec || ')'
WHEN data_type = 'NUMERIC'
THEN 'NUMERIC(' || prec || ',' || scale || ')'
ELSE ''
END 
  || ' COMMENT ''' || comment || ''';' FROM db_attribute WHERE owner_name = 'RMS_COM' AND comment IS NOT null ;
