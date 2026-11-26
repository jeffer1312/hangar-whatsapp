package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

const accountA = "556411112222"

func TestAvatarFetchIsSkippedWhileTheCacheIsFresh(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	if id, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0); err != nil || id != "" {
		t.Fatalf("first claim = %q, %v; want a fetch with no known picture", id, err)
	}
	if err := db.SaveAvatar(ctx, accountA, chatA, Avatar{State: AvatarOK, PictureID: "P1", Image: []byte{0xFF, 0xD8, 0xFF}}, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0.Add(23*time.Hour)); !errors.Is(err, ErrAvatarFresh) {
		t.Fatalf("claim within 24 h = %v, want ErrAvatarFresh", err)
	}
	if id, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0.Add(24*time.Hour)); err != nil || id != "P1" {
		t.Fatalf("claim after 24 h = %q, %v; want the stored picture id P1", id, err)
	}
	for _, state := range []string{AvatarNone, AvatarHidden} {
		if err := db.SaveAvatar(ctx, accountA, chatA, Avatar{State: state}, t0); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0.Add(time.Hour)); !errors.Is(err, ErrAvatarFresh) {
			t.Fatalf("%s claim within 24 h = %v, want ErrAvatarFresh", state, err)
		}
		if id, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0.Add(25*time.Hour)); err != nil || id != "" {
			t.Fatalf("%s claim after 24 h = %q, %v; want a fetch without picture id", state, id, err)
		}
	}
}

func TestAvatarRenewKeepsThePicture(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	img := []byte{0xFF, 0xD8, 0xFF, 1}
	if err := db.SaveAvatar(ctx, accountA, chatA, Avatar{State: AvatarOK, PictureID: "P1", Image: img}, t0); err != nil {
		t.Fatal(err)
	}
	later := t0.Add(25 * time.Hour)
	if err := db.RenewAvatar(ctx, accountA, chatA, later); err != nil {
		t.Fatal(err)
	}
	a, err := db.Avatar(ctx, accountA, chatA)
	if err != nil || a.State != AvatarOK || a.PictureID != "P1" || !bytes.Equal(a.Image, img) || a.FetchedAt != "2026-11-25T13:00:00Z" {
		t.Fatalf("renewed avatar = %+v, %v", a, err)
	}
}

func TestAvatarIsKeptPerAccount(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	img := []byte{0xFF, 0xD8, 0xFF}
	if err := db.SaveAvatar(ctx, accountA, chatA, Avatar{State: AvatarOK, PictureID: "P1", Image: img}, t0); err != nil {
		t.Fatal(err)
	}
	if got, err := db.ChatAvatar(ctx, accountA, chatA); err != nil || !bytes.Equal(got, img) {
		t.Fatalf("ChatAvatar for the account that fetched = %v, %v", got, err)
	}
	other := "5511988887777"
	if got, err := db.ChatAvatar(ctx, other, chatA); err != nil || got != nil {
		t.Fatalf("ChatAvatar for another number = %v, %v; want nil", got, err)
	}
	if _, err := db.ClaimAvatarFetch(ctx, other, chatA, t0); err != nil {
		t.Fatalf("another number claims its own fetch = %v", err)
	}
	if err := db.SaveAvatar(ctx, accountA, chatA, Avatar{State: AvatarHidden}, t0); err != nil {
		t.Fatal(err)
	}
	if got, err := db.ChatAvatar(ctx, accountA, chatA); err != nil || got != nil {
		t.Fatalf("ChatAvatar of a hidden picture = %v, %v; want nil", got, err)
	}
}

func TestAvatarFetchesCapAt30A24Hours(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	for i := range 30 {
		if _, err := db.ClaimAvatarFetch(ctx, accountA, fmt.Sprintf("55640000%04d@s.whatsapp.net", i), t0.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("claim %d = %v", i, err)
		}
	}
	if _, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0.Add(time.Hour)); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("31st claim in 24 h = %v, want ErrRateLimited", err)
	}
	// A failed fetch writes no avatar, but its attempt still counts.
	if _, err := db.ClaimAvatarFetch(ctx, accountA, chatA, t0.Add(24*time.Hour)); err != nil {
		t.Fatalf("claim once the first left the window = %v", err)
	}
}
