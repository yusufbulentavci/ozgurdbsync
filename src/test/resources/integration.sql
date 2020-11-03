create user testuser;
\password testuser;
1
create database test1;
\c test1
create schema sc1;
create table sc1.tbl1(id int primary key, tckn text, ad text);
insert into sc1.tbl1 values (1, 'tckn1', 'ad1');
insert into sc2.tbl1 values (2, 'tckn2', 'ad2');

create database test2;
\c test2
create schema sc1;
create table sc1.tbl1(id int primary key, tckn text, ad text);
insert into sc1.tbl1 values (2, 'tckn2', 'ad2');
insert into sc1.tbl1 values (3, 'tckn3', 'ad3');

