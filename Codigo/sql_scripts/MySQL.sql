/*==============================================================*/
/* DBMS name:      MySQL 8.x                                    */
/* Created on:     19/05/2025 15:53:25                          */
/*==============================================================*/

/*==============================================================*/
/* Table: AGE_RATINGS                                           */
/*==============================================================*/
create table AGE_RATINGS
(
   AGE_RATING_ID        int not null  comment '',
   AGE_RATING_DESIGNATION varchar(20) not null  comment '',
   primary key (AGE_RATING_ID)
);

/*==============================================================*/
/* Table: CONTENTS                                              */
/*==============================================================*/
create table CONTENTS
(
   CONTENT_CODE         varchar(16) not null  comment '',
   TYPE_ID              int not null  comment '',
   AGE_RATING_ID        int not null  comment '',
   DIRECTOR_ID          int not null  comment '',
   TITLE                varchar(200) not null  comment '',
   RELEASE_DATE         date not null  comment '',
   DURATION             int not null  comment '',
   primary key (CONTENT_CODE)
);

/*==============================================================*/
/* Table: CONTENT_GENRES                                        */
/*==============================================================*/
create table CONTENT_GENRES
(
   CONTENT_CODE         varchar(16) not null  comment '',
   GENRE_ID             int not null  comment '',
   primary key (CONTENT_CODE, GENRE_ID)
);

/*==============================================================*/
/* Table: DIRECTORS                                             */
/*==============================================================*/
create table DIRECTORS
(
   DIRECTOR_ID          int not null  comment '',
   NAME                 varchar(100) not null  comment '',
   primary key (DIRECTOR_ID)
);

/*==============================================================*/
/* Table: GENRES                                                */
/*==============================================================*/
create table GENRES
(
   GENRE_ID             int not null  comment '',
   GENRE_DESIGNATION    varchar(20) not null  comment '',
   primary key (GENRE_ID)
);

/*==============================================================*/
/* Table: TYPES                                                 */
/*==============================================================*/
create table TYPES
(
   TYPE_ID              int not null  comment '',
   TYPE_DESIGNATION     varchar(20) not null  comment '',
   primary key (TYPE_ID)
);

alter table CONTENTS add constraint FK_CONTENTS_CONTENT_A_AGE_RATI foreign key (AGE_RATING_ID)
      references AGE_RATINGS (AGE_RATING_ID) on delete restrict on update restrict;

alter table CONTENTS add constraint FK_CONTENTS_CONTENT_D_DIRECTOR foreign key (DIRECTOR_ID)
      references DIRECTORS (DIRECTOR_ID) on delete restrict on update restrict;

alter table CONTENTS add constraint FK_CONTENTS_CONTENT_T_TYPES foreign key (TYPE_ID)
      references TYPES (TYPE_ID) on delete restrict on update restrict;

alter table CONTENT_GENRES add constraint FK_CONTENT__CONTENT_G_CONTENTS foreign key (CONTENT_CODE)
      references CONTENTS (CONTENT_CODE) on delete restrict on update restrict;

alter table CONTENT_GENRES add constraint FK_CONTENT__CONTENT_G_GENRES foreign key (GENRE_ID)
      references GENRES (GENRE_ID) on delete restrict on update restrict;

