/*==============================================================*/
/* DBMS name:      PostgreSQL 14.x                              */
/* Created on:     19/05/2025 15:47:34                          */
/*==============================================================*/

/*==============================================================*/
/* Table: AGE_GROUPS                                            */
/*==============================================================*/
create table AGE_GROUPS (
   AGE_GROUP_ID         integer              not null,
   AGE_GROUP_DESIGNATION varchar(20)          not null,
   constraint PK_AGE_GROUPS primary key (AGE_GROUP_ID)
);

/*==============================================================*/
/* Index: AGE_GROUPS_PK                                         */
/*==============================================================*/
create unique index AGE_GROUPS_PK on AGE_GROUPS (
AGE_GROUP_ID
);

/*==============================================================*/
/* Table: COUNTRIES                                             */
/*==============================================================*/
create table COUNTRIES (
   COUNTRY_ID           integer              not null,
   COUNTRY_DESIGNATION  varchar(20)          not null,
   constraint PK_COUNTRIES primary key (COUNTRY_ID)
);

/*==============================================================*/
/* Index: COUNTRIES_PK                                          */
/*==============================================================*/
create unique index COUNTRIES_PK on COUNTRIES (
COUNTRY_ID
);

/*==============================================================*/
/* Table: GENDERS                                               */
/*==============================================================*/
create table GENDERS (
   GENDER_ID            integer              not null,
   GENDER_DESIGNATION   varchar(20)          not null,
   constraint PK_GENDERS primary key (GENDER_ID)
);

/*==============================================================*/
/* Index: GENDERS_PK                                            */
/*==============================================================*/
create unique index GENDERS_PK on GENDERS (
GENDER_ID
);

/*==============================================================*/
/* Table: SUBSCRIPTION_STATUS                                   */
/*==============================================================*/
create table SUBSCRIPTION_STATUS (
   SUBSCRIPTION_STATUS_ID integer              not null,
   SUBSCRIPTION_STATUS_DESIGNATION varchar(20)          not null,
   constraint PK_SUBSCRIPTION_STATUS primary key (SUBSCRIPTION_STATUS_ID)
);

/*==============================================================*/
/* Index: SUBSCRIPTION_STATUS_PK                                */
/*==============================================================*/
create unique index SUBSCRIPTION_STATUS_PK on SUBSCRIPTION_STATUS (
SUBSCRIPTION_STATUS_ID
);

/*==============================================================*/
/* Table: USERS                                                 */
/*==============================================================*/
create table USERS (
   USER_CODE            varchar(16)          not null,
   AGE_GROUP_ID         integer              not null,
   GENDER_ID            integer              not null,
   COUNTRY_ID           integer              not null,
   SUBSCRIPTION_STATUS_ID integer              not null,
   NAME                 varchar(100)         not null,
   EMAIL                varchar(150)         not null,
   SIGNUP_DATE          date                 not null,
   DISTRICT             varchar(50)          not null,
   CITY                 varchar(50)          not null,
   POSTAL_CODE          varchar(12)          not null,
   STREET_ADDRESS       varchar(150)         not null,
   constraint PK_USERS primary key (USER_CODE)
);

/*==============================================================*/
/* Index: USERS_PK                                              */
/*==============================================================*/
create unique index USERS_PK on USERS (
USER_CODE
);

/*==============================================================*/
/* Index: USER_AGE_GROUP_FK                                     */
/*==============================================================*/
create  index USER_AGE_GROUP_FK on USERS (
AGE_GROUP_ID
);

/*==============================================================*/
/* Index: USER_GENDER_FK                                        */
/*==============================================================*/
create  index USER_GENDER_FK on USERS (
GENDER_ID
);

/*==============================================================*/
/* Index: USER_COUNTRY_FK                                       */
/*==============================================================*/
create  index USER_COUNTRY_FK on USERS (
COUNTRY_ID
);

/*==============================================================*/
/* Index: USER_SUBSCRIPTION_STATUS_FK                           */
/*==============================================================*/
create  index USER_SUBSCRIPTION_STATUS_FK on USERS (
SUBSCRIPTION_STATUS_ID
);

alter table USERS
   add constraint FK_USERS_USER_AGE__AGE_GROU foreign key (AGE_GROUP_ID)
      references AGE_GROUPS (AGE_GROUP_ID)
      on delete restrict on update restrict;

alter table USERS
   add constraint FK_USERS_USER_COUN_COUNTRIE foreign key (COUNTRY_ID)
      references COUNTRIES (COUNTRY_ID)
      on delete restrict on update restrict;

alter table USERS
   add constraint FK_USERS_USER_GEND_GENDERS foreign key (GENDER_ID)
      references GENDERS (GENDER_ID)
      on delete restrict on update restrict;

alter table USERS
   add constraint FK_USERS_USER_SUBS_SUBSCRIP foreign key (SUBSCRIPTION_STATUS_ID)
      references SUBSCRIPTION_STATUS (SUBSCRIPTION_STATUS_ID)
      on delete restrict on update restrict;

