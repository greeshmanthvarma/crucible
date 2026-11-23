from collections import Counter

import select_pool


def test_pool_draw_is_seeded_and_balanced() -> None:
    projects = {
        "astropy__astropy": "astropy",
        "pytest-dev__pytest": "pytest",
        "scikit-learn__scikit-learn": "scikit-learn",
        "sphinx-doc__sphinx": "sphinx",
        "sympy__sympy": "sympy",
    }
    excluded = {f"{prefix}-1" for prefix in projects}
    candidates = [
        select_pool.PoolCase(f"{prefix}-{number}", project, lines)
        for prefix, project in projects.items()
        for number, lines in enumerate((12, 14, 41, 40, 1, 22, 18, 35, 27))
    ]

    pool, remainder = select_pool.draw_pool(candidates, excluded)
    assert (pool, remainder) == select_pool.draw_pool(
        list(reversed(candidates)), excluded
    )
    assert Counter(case.project for case in pool) == dict.fromkeys(projects.values(), 5)
    assert Counter(case.project for case in remainder) == dict.fromkeys(
        projects.values(), 2
    )
    assert len({case.case_id for case in pool + remainder}) == 35
    assert not excluded.intersection(case.case_id for case in pool + remainder)
    assert all(case.patch_changed_lines <= 40 for case in pool + remainder)
