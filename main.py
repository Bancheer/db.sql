from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result



def select_03():
    """
    SELECT 
        s.id, 
        s.fullname, 
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    WHERE g.subjects_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(desc('average_grade')).limit(1).all()
    return result


def select_04():
    """
    SELECT ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    """
    subquery = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')).subquery()
    result = session.query(subquery.c.average_grade).all()
    return result


def select_05():
    """
    SELECT s.name
    FROM subjects s
    JOIN teachers t ON s.teacher_id = t.id
    WHERE t.fullname = 'Donald Stone';
    """
    result = session.query(Subject.name) \
        .join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(Teacher.fullname == 'Donald Stone').all()
    return result



def select_06():
    """
    SELECT fullname
    FROM students s
    WHERE group_id = 3;
    """
    result = session.query(Student.fullname) \
        .filter(Student.group_id == 3).all()
    return result



def select_07():
    """
    SELECT g.grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    WHERE s.group_id = 2 AND g.subjects_id = 3;
    """
    result = session.query(Grade.grade) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(Student.group_id == 2, Grade.subjects_id == 3).all()
    return result



def select_08():
    """
    SELECT AVG(g.grade) AS average_grade
    FROM grades g
    JOIN subjects s ON g.subject_id = s.id
    JOIN teachers t ON s.teacher_id = t.id
    WHERE t.fullname = 'Lisa Brown';
    """
    result = session.query(func.avg(Grade.grade).label('average_grade')) \
        .join(Subject, Grade.subjects_id == Subject.id) \
        .join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(Teacher.fullname == 'Lisa Brown').all()
    return result


def select_09():
    """
    SELECT DISTINCT subjects.name
    FROM subjects
    JOIN students ON subjects.id = subject_id;
    """
    result = session.query(Subject.name).distinct() \
        .join(Student, Subject.id == Subject.id).all()
    return result


def select_10():
    """
    SELECT subjects.name
    FROM subjects
    JOIN students ON subjects.id = students.subject_id
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE students.fullname = 'Heidi Moore' AND teachers.fullname = 'Lisa Brown';
    """
    result = session.query(Subject.name) \
        .join(Student, Subject.id == Subject.id) \
        .join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(Student.fullname == 'Heidi Moore', Teacher.fullname == 'Lisa Brown').all()
    return result


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


# if __name__ == '__main__':
#     print(select_01())
#     print(select_02())
#     print(select_03())
#     print(select_04())
#     print(select_05())
#     print(select_06())
#     print(select_07())
#     print(select_08())
#     print(select_09())
#     print(select_10())
#     print(select_12())