package stringy

func Add(s []string, a string) []string {
	for _, existing := range s {
		if a == existing {
			return s
		}
	}
	return append(s, a)

}

func Union(s []string, a []string) []string {
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry == existing {
				found = true
				break
			}
		}
		if !found {
			s = append(s, entry)
		}
	}
	return s
}

// Uniq 去重
func Uniq(s []string) (r []string) {
outerLoop:
	for _, entry := range s {
		for _, existing := range r {
			if existing == entry {
				continue outerLoop
			}
		}
		r = append(r, entry)
	}
	return
}
