delete from T_USERINFO;

insert into T_USERINFO(username,passwd,appname,keyid) values('广州理工','123456','教学综合信息服务平台',SEQ_USERINFO.nextval);
insert into T_USERINFO(username,passwd,appname,keyid) values('liang','101018','天气查询APP',SEQ_USERINFO.nextval);

exit;
