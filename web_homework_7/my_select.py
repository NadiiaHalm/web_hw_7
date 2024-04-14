from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """"
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
        s.fullname ,
        AVG(g.grade) AS overall_average_grade
    FROM grades g
    JOIN students s  ON s.id = g.student_id
    WHERE g.subject_id = 1
    GROUP BY
        s.id,
        s.fullname
    ORDER BY overall_average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id, Student.fullname) \
        .order_by(desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        g.id,
        g.name,
        ROUND(AVG(grade), 2) AS average_grade
    FROM students s
    JOIN groups g ON s.group_id = g.id
    JOIN grades gr ON s.id = gr.student_id
    WHERE gr.subject_id = 1
    GROUP BY
        g.id,
        g.name;
    """
    result = session.query(Group.id, Group.name, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Group).join(Grade).filter(Grade.subjects_id == 1) \
        .group_by(Group.id).all()
    return result


def select_04():
    """
    SELECT ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).first()
    return result


def select_05():
    """
    SELECT name
    FROM subjects
    WHERE teacher_id = 5;
    """
    result = session.query(Subject.name).select_from(Subject).filter(Subject.teacher_id == 2).all()
    return result


def select_06():
    """
    SELECT id, fullname
    FROM students
    WHERE group_id = 1;
    """
    result = session.query(Student.id, Student.fullname).select_from(Student).filter(Student.group_id == 1).all()
    return result


def select_07():
    """
    SELECT
        s.fullname,
        g.grade,
        g.date_received
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects subj ON g.subject_id = subj.id
    WHERE s.group_id = 1
        AND subj.id = 1;
    """
    result = session.query(Student.fullname, Grade.grade, Grade.grade_date).select_from(Student) \
        .join(Grade).join(Subject).filter(Student.group_id == 1, Subject.id == 1).all()
    return result


def select_08():
    """
    SELECT
        t.name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM teachers t
    JOIN subjects s ON t.id = s.teacher_id
    JOIN grades g ON s.id = g.subject_id
    GROUP BY t.name;
    """
    result = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Teacher).join(Subject).join(Grade).group_by(Teacher.fullname).all()
    return result


def select_09():
    """
    SELECT subj.name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects subj ON g.subject_id = subj.id
    WHERE s.id = 1;
    """
    result = session.query(Subject.name).select_from(Student).join(Grade).join(Subject).filter(Student.id == 1).all()
    return result


def select_10():
    """
    SELECT subj.name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects subj ON g.subject_id = subj.id
    JOIN teachers t ON subj.teacher_id = t.id
    WHERE
        s.id = 1
        AND t.id = 1;
    """
    result = session.query(Subject.name).select_from(Student).join(Grade).join(Subject).join(Teacher) \
        .filter(Student.id == 1, Teacher.id == 1).all()
    return result


if __name__ == '__main__':
    print(select_10())
