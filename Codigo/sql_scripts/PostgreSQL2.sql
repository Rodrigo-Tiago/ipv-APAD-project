/*==============================================================*/
/* DBMS name:      PostgreSQL 14.x                              */
/* Created on:     19/05/2025 15:51:21                          */
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
/* Table: AGE_RESTRICTIONS                                      */
/*==============================================================*/
create table AGE_RESTRICTIONS (
   AGE_RESTRICTION_ID   integer              not null,
   AGE_RESTRICTION_DESIGNATION varchar(20)          not null,
   constraint PK_AGE_RESTRICTIONS primary key (AGE_RESTRICTION_ID)
);

/*==============================================================*/
/* Index: AGE_RESTRICTIONS_PK                                   */
/*==============================================================*/
create unique index AGE_RESTRICTIONS_PK on AGE_RESTRICTIONS (
AGE_RESTRICTION_ID
);

/*==============================================================*/
/* Table: CATEGORIES                                            */
/*==============================================================*/
create table CATEGORIES (
   CATEGORY_ID          integer              not null,
   CATEGORY_DESIGNATION varchar(20)          not null,
   constraint PK_CATEGORIES primary key (CATEGORY_ID)
);

/*==============================================================*/
/* Index: CATEGORIES_PK                                         */
/*==============================================================*/
create unique index CATEGORIES_PK on CATEGORIES (
CATEGORY_ID
);

/*==============================================================*/
/* Table: CONTENTS                                              */
/*==============================================================*/
create table CONTENTS (
   CONTENT_CODE         varchar(16)          not null,
   TYPE_ID              integer              not null,
   AGE_RESTRICTION_ID   integer              not null,
   DIRECTOR_ID          integer              not null,
   TITLE                varchar(200)         not null,
   RELEASE_DATE         date                 not null,
   DURATION             integer              not null,
   constraint PK_CONTENTS primary key (CONTENT_CODE)
);

/*==============================================================*/
/* Index: CONTENTS_PK                                           */
/*==============================================================*/
create unique index CONTENTS_PK on CONTENTS (
CONTENT_CODE
);

/*==============================================================*/
/* Index: CONTENT_TYPE_FK                                       */
/*==============================================================*/
create  index CONTENT_TYPE_FK on CONTENTS (
TYPE_ID
);

/*==============================================================*/
/* Index: CONTENT_AGE_RESTRICTION_FK                            */
/*==============================================================*/
create  index CONTENT_AGE_RESTRICTION_FK on CONTENTS (
AGE_RESTRICTION_ID
);

/*==============================================================*/
/* Index: CONTENT_DIRECTOR_FK                                   */
/*==============================================================*/
create  index CONTENT_DIRECTOR_FK on CONTENTS (
DIRECTOR_ID
);

/*==============================================================*/
/* Table: CONTENT_CATEGORIES                                    */
/*==============================================================*/
create table CONTENT_CATEGORIES (
   CATEGORY_ID          integer              not null,
   CONTENT_CODE         varchar(16)          not null,
   constraint PK_CONTENT_CATEGORIES primary key (CATEGORY_ID, CONTENT_CODE)
);

/*==============================================================*/
/* Index: CONTENT_CATEGORIES_PK                                 */
/*==============================================================*/
create unique index CONTENT_CATEGORIES_PK on CONTENT_CATEGORIES (
CATEGORY_ID,
CONTENT_CODE
);

/*==============================================================*/
/* Index: CONTENT_CATEGORIES2_FK                                */
/*==============================================================*/
create  index CONTENT_CATEGORIES2_FK on CONTENT_CATEGORIES (
CONTENT_CODE
);

/*==============================================================*/
/* Index: CONTENT_CATEGORIES_FK                                 */
/*==============================================================*/
create  index CONTENT_CATEGORIES_FK on CONTENT_CATEGORIES (
CATEGORY_ID
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
/* Table: DIRECTORS                                             */
/*==============================================================*/
create table DIRECTORS (
   DIRECTOR_ID          integer              not null,
   NAME                 varchar(100)         not null,
   constraint PK_DIRECTORS primary key (DIRECTOR_ID)
);

/*==============================================================*/
/* Index: DIRECTORS_PK                                          */
/*==============================================================*/
create unique index DIRECTORS_PK on DIRECTORS (
DIRECTOR_ID
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
/* Table: SESSIONS                                              */
/*==============================================================*/
create table SESSIONS (
   SESSION_CODE         varchar(16)          not null,
   USER_CODE            varchar(16)          not null,
   CONTENT_CODE         varchar(16)          not null,
   TIME_                timestamp            not null,
   WATCHED_DURATION     integer              not null,
   PLATFORM             varchar(20)          not null,
   DEVICE_TYPE          varchar(20)          not null,
   OS_FAMILY            varchar(20)          not null,
   OS_NAME              varchar(20)          not null,
   APP_VERSION          varchar(20)          not null,
   constraint PK_SESSIONS primary key (SESSION_CODE)
);

/*==============================================================*/
/* Index: SESSIONS_PK                                           */
/*==============================================================*/
create unique index SESSIONS_PK on SESSIONS (
SESSION_CODE
);

/*==============================================================*/
/* Index: SESSION_USER_FK                                       */
/*==============================================================*/
create  index SESSION_USER_FK on SESSIONS (
USER_CODE
);

/*==============================================================*/
/* Index: SESSION_CONTENT_FK                                    */
/*==============================================================*/
create  index SESSION_CONTENT_FK on SESSIONS (
CONTENT_CODE
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
/* Table: TYPES                                                 */
/*==============================================================*/
create table TYPES (
   TYPE_ID              integer              not null,
   TYPE_DESIGNATION     varchar(20)          not null,
   constraint PK_TYPES primary key (TYPE_ID)
);

/*==============================================================*/
/* Index: TYPES_PK                                              */
/*==============================================================*/
create unique index TYPES_PK on TYPES (
TYPE_ID
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

alter table CONTENTS
   add constraint FK_CONTENTS_CONTENT_A_AGE_REST foreign key (AGE_RESTRICTION_ID)
      references AGE_RESTRICTIONS (AGE_RESTRICTION_ID)
      on delete restrict on update restrict;

alter table CONTENTS
   add constraint FK_CONTENTS_CONTENT_D_DIRECTOR foreign key (DIRECTOR_ID)
      references DIRECTORS (DIRECTOR_ID)
      on delete restrict on update restrict;

alter table CONTENTS
   add constraint FK_CONTENTS_CONTENT_T_TYPES foreign key (TYPE_ID)
      references TYPES (TYPE_ID)
      on delete restrict on update restrict;

alter table CONTENT_CATEGORIES
   add constraint FK_CONTENT__CONTENT_C_CATEGORI foreign key (CATEGORY_ID)
      references CATEGORIES (CATEGORY_ID)
      on delete restrict on update restrict;

alter table CONTENT_CATEGORIES
   add constraint FK_CONTENT__CONTENT_C_CONTENTS foreign key (CONTENT_CODE)
      references CONTENTS (CONTENT_CODE)
      on delete restrict on update restrict;

alter table SESSIONS
   add constraint FK_SESSIONS_SESSION_C_CONTENTS foreign key (CONTENT_CODE)
      references CONTENTS (CONTENT_CODE)
      on delete restrict on update restrict;

alter table SESSIONS
   add constraint FK_SESSIONS_SESSION_U_USERS foreign key (USER_CODE)
      references USERS (USER_CODE)
      on delete restrict on update restrict;

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

