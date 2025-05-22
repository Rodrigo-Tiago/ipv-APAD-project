/*==============================================================*/
/* DBMS name:      Microsoft SQL Server 2022                    */
/* Created on:     19/05/2025 16:07:09                          */
/*==============================================================*/


if exists (select 1
   from sys.sysreferences r join sys.sysobjects o on (o.id = r.constid and o.type = 'F')
   where r.fkeyid = object_id('SESSIONS') and o.name = 'FK_SESSIONS_SESSION_C_CONTENTS')
alter table SESSIONS
   drop constraint FK_SESSIONS_SESSION_C_CONTENTS

if exists (select 1
   from sys.sysreferences r join sys.sysobjects o on (o.id = r.constid and o.type = 'F')
   where r.fkeyid = object_id('SESSIONS') and o.name = 'FK_SESSIONS_SESSION_D_DEVICES')
alter table SESSIONS
   drop constraint FK_SESSIONS_SESSION_D_DEVICES

if exists (select 1
   from sys.sysreferences r join sys.sysobjects o on (o.id = r.constid and o.type = 'F')
   where r.fkeyid = object_id('SESSIONS') and o.name = 'FK_SESSIONS_SESSION_T_TIMES')
alter table SESSIONS
   drop constraint FK_SESSIONS_SESSION_T_TIMES

if exists (select 1
   from sys.sysreferences r join sys.sysobjects o on (o.id = r.constid and o.type = 'F')
   where r.fkeyid = object_id('SESSIONS') and o.name = 'FK_SESSIONS_SESSION_U_USERS')
alter table SESSIONS
   drop constraint FK_SESSIONS_SESSION_U_USERS

if exists (select 1
            from  sysobjects
           where  id = object_id('CONTENTS')
            and   type = 'U')
   drop table CONTENTS

if exists (select 1
            from  sysobjects
           where  id = object_id('DEVICES')
            and   type = 'U')
   drop table DEVICES

if exists (select 1
            from  sysindexes
           where  id    = object_id('SESSIONS')
            and   name  = 'SESSION_TIME_FK'
            and   indid > 0
            and   indid < 255)
   drop index SESSIONS.SESSION_TIME_FK

if exists (select 1
            from  sysindexes
           where  id    = object_id('SESSIONS')
            and   name  = 'SESSION_DEVICE_FK'
            and   indid > 0
            and   indid < 255)
   drop index SESSIONS.SESSION_DEVICE_FK

if exists (select 1
            from  sysindexes
           where  id    = object_id('SESSIONS')
            and   name  = 'SESSION_CONTENT_FK'
            and   indid > 0
            and   indid < 255)
   drop index SESSIONS.SESSION_CONTENT_FK

if exists (select 1
            from  sysindexes
           where  id    = object_id('SESSIONS')
            and   name  = 'SESSION_USER_FK'
            and   indid > 0
            and   indid < 255)
   drop index SESSIONS.SESSION_USER_FK

if exists (select 1
            from  sysobjects
           where  id = object_id('SESSIONS')
            and   type = 'U')
   drop table SESSIONS

if exists (select 1
            from  sysobjects
           where  id = object_id('TIMES')
            and   type = 'U')
   drop table TIMES

if exists (select 1
            from  sysobjects
           where  id = object_id('USERS')
            and   type = 'U')
   drop table USERS

/*==============================================================*/
/* Table: CONTENTS                                              */
/*==============================================================*/
create table CONTENTS (
   CONTENT_ID           int                  not null,
   CONTENT_CODE         varchar(16)          not null,
   SOURCE               varchar(15)          not null,
   TITLE                varchar(200)         not null,
   GENRES               text                 not null,
   RELEASE_DATE         datetime             not null,
   TYPE                 varchar(20)          not null,
   DURATION             int                  not null,
   AGE_RATING           varchar(20)          not null,
   DIRECTOR             varchar(100)         not null,
   constraint PK_CONTENTS primary key (CONTENT_ID)
)

/*==============================================================*/
/* Table: DEVICES                                               */
/*==============================================================*/
create table DEVICES (
   DEVICE_ID            int  identity(1,1)   not null,
   PLATFORM             varchar(20)          not null,
   DEVICE_TYPE          varchar(20)          not null,
   OS_FAMILY            varchar(20)          not null,
   OS_NAME              varchar(20)          not null,
   APP_VERSION          varchar(20)          not null,
   constraint PK_DEVICES primary key (DEVICE_ID)
)

/*==============================================================*/
/* Table: SESSIONS                                              */
/*==============================================================*/
create table SESSIONS (
   SESSION_ID           int                  not null,
   USER_ID              int                  not null,
   CONTENT_ID           int                  not null,
   DEVICE_ID            int                  not null,
   TIME_ID              int                  not null,
   SESSION_CODE         varchar(16)          not null,
   SOURCE               varchar(15)          not null,
   WATCHED_DURATION     int                  not null,
   WATCHED_PERCENT      float                not null,
   constraint PK_SESSIONS primary key (SESSION_ID)
)

/*==============================================================*/
/* Index: SESSION_USER_FK                                       */
/*==============================================================*/




create nonclustered index SESSION_USER_FK on SESSIONS (USER_ID ASC)

/*==============================================================*/
/* Index: SESSION_CONTENT_FK                                    */
/*==============================================================*/




create nonclustered index SESSION_CONTENT_FK on SESSIONS (CONTENT_ID ASC)

/*==============================================================*/
/* Index: SESSION_DEVICE_FK                                     */
/*==============================================================*/




create nonclustered index SESSION_DEVICE_FK on SESSIONS (DEVICE_ID ASC)

/*==============================================================*/
/* Index: SESSION_TIME_FK                                       */
/*==============================================================*/




create nonclustered index SESSION_TIME_FK on SESSIONS (TIME_ID ASC)

/*==============================================================*/
/* Table: TIMES                                                 */
/*==============================================================*/
create table TIMES (
   TIME_ID              int                  not null,
   DAY                  smallint             not null,
   WEEK                 smallint             not null,
   MONTH                smallint             not null,
   YEAR                 smallint             not null,
   HOUR                 smallint             not null,
   MINUTE               smallint             not null,
   DAY_NAME             varchar(20)          not null,
   MONTH_NAME           varchar(20)          not null,
   constraint PK_TIMES primary key (TIME_ID)
)

/*==============================================================*/
/* Table: USERS                                                 */
/*==============================================================*/
create table USERS (
   USER_ID              int                  not null,
   USER_CODE            varchar(16)          not null,
   SOURCE               varchar(15)          not null,
   NAME                 varchar(100)         not null,
   AGE_GROUP            varchar(20)          not null,
   GENDER               varchar(20)          not null,
   SIGNUP_DATE          datetime             not null,
   SUBSCRIPTION_STATUS  varchar(20)          not null,
   COUNTRY              varchar(20)          not null,
   DISTRICT             varchar(50)          not null,
   CITY                 varchar(50)          not null,
   POSTAL_CODE          varchar(12)          not null,
   STREET_ADDRESS       varchar(150)         not null,
   constraint PK_USERS primary key (USER_ID)
)

alter table SESSIONS
   add constraint FK_SESSIONS_SESSION_C_CONTENTS foreign key (CONTENT_ID)
      references CONTENTS (CONTENT_ID)

alter table SESSIONS
   add constraint FK_SESSIONS_SESSION_D_DEVICES foreign key (DEVICE_ID)
      references DEVICES (DEVICE_ID)

alter table SESSIONS
   add constraint FK_SESSIONS_SESSION_T_TIMES foreign key (TIME_ID)
      references TIMES (TIME_ID)

alter table SESSIONS
   add constraint FK_SESSIONS_SESSION_U_USERS foreign key (USER_ID)
      references USERS (USER_ID)