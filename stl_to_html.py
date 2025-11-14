def generate_cad_thumbnail(request, ring_id):
    """Generate interactive HTML visualizations for a ring's CAD STL files.

    For the given ring, this view:
    - Collects up to three STL CAD files (cad_file, cad_file_2, cad_file_3).
    - Checks if corresponding HTML visualizations already exist on disk and
      returns early when possible.
    - Otherwise, converts each STL binary to a temporary .stl file, loads it
      into a mesh, and builds a Plotly Mesh3d visualization with a simple
      shadow effect.
    - Saves each visualization as an HTML file under MEDIA_ROOT and stores
      the URL back onto the ring model (cad_file_thumbnail fields).

    The response is a plain-text HttpResponse containing a list of the HTML
    URLs (one per CAD file slot).
    """

    # Grab the ring (and its CAD fields) or 404 if it doesn't exist.
    ring = get_object_or_404(Rings, ring_id=ring_id)

    # Build a list of CAD files we actually have for this ring.
    rings_list = [ring.cad_file]
    if ring.cad_file_2:
        rings_list.append(ring.cad_file_2)
    if ring.cad_file_3:
        rings_list.append(ring.cad_file_3)

    # Precompute file paths and URLs for all three possible HTML outputs.
    html_file_path_2 = os.path.join(settings.MEDIA_ROOT, f"cad_file_{ring_id}_2.html")
    html_file_url_2 = os.path.join(settings.MEDIA_URL, f"cad_file_{ring_id}_2.html")

    html_file_path_3 = os.path.join(settings.MEDIA_ROOT, f"cad_file_{ring_id}_3.html")
    html_file_url_3 = os.path.join(settings.MEDIA_URL, f"cad_file_{ring_id}_3.html")

    html_file_path = os.path.join(settings.MEDIA_ROOT, f"cad_file_{ring_id}.html")
    html_file_url = os.path.join(settings.MEDIA_URL, f"cad_file_{ring_id}.html")

    # If the first CAD HTML is already on disk and there are no extra CAD files, just return that URL.
    if os.path.exists(html_file_path):
        if not ring.cad_file_2:
            return HttpResponse(html_file_url, content_type="text/plain")

    # If we already have HTML for the second CAD file and there is no third, return the primary URL (keeps old behavior).
    elif os.path.exists(html_file_path_2):
        if not ring.cad_file_3:
            return HttpResponse(html_file_url, content_type="text/plain")

    # If there's HTML for the third CAD file, return the primary URL as well (same legacy behavior).
    elif os.path.exists(html_file_path_3):
        return HttpResponse(html_file_url, content_type="text/plain")

    # Otherwise, we need to actually build the HTML visualizations from the STL data.
    else:
        # Use a non-interactive Matplotlib backend so we don't need any GUI on the server.
        plt.switch_backend("Agg")

        # Pull just the CAD fields in a lean query so we don't load extra stuff.
        stl_instance = Rings.objects.only("cad_file", "cad_file_2", "cad_file_3").get(
            ring_id=ring_id
        )

        # Loop through each CAD file slot we actually have for this ring.
        for index, cad_file in enumerate(rings_list):
            # Map the index over to the right CAD field on the ring instance.
            if index == 0:
                stl_data = stl_instance.cad_file
            elif index == 1:
                stl_data = stl_instance.cad_file_2
            elif index == 2:
                stl_data = stl_instance.cad_file_3
            else:
                continue

            # Turn the binary field into a temporary STL file on disk.
            cad_file_bytes = bytes(stl_data)

            if index == 0:
                stl_file_path = os.path.join(settings.MEDIA_ROOT, f"stl_file_{ring_id}.stl")
            elif index == 1:
                stl_file_path = os.path.join(settings.MEDIA_ROOT, f"stl_file_{ring_id}_2.stl")
            elif index == 2:
                stl_file_path = os.path.join(settings.MEDIA_ROOT, f"stl_file_{ring_id}_3.stl")

            save_stl_from_binary(cad_file_bytes, stl_file_path)

            # Load the STL mesh from disk, then clean up the temp file.
            mesh_data = mesh.Mesh.from_file(stl_file_path)
            os.remove(stl_file_path)

            # Map the ring's metal type to a hex color for rendering.
            if "Yellow Gold" in ring.metal:
                color = "#FFD700"
            elif "White Gold" in ring.metal:
                color = "#E1E1E1"
            elif "Tungsten" in ring.metal:
                color = "#666565"
            elif "Ceramic" in ring.metal:
                color = "#2B2B2B"
            elif "Zirconium" in ring.metal:
                color = "#525252"
            elif "Rose Gold" in ring.metal:
                color = "#ffcab3"
            elif "Platinum" in ring.metal:
                color = "#E5E4E2"
            else:
                color = "#808080"

            # Prep the vertices and faces arrays for the Plotly Mesh3d.
            vertices = mesh_data.vectors.reshape(-1, 3)
            faces = np.arange(len(vertices)).reshape(-1, 3)

            # Compute the ranges on each axis so we can keep the aspect ratio sane.
            x_range = vertices[:, 0].max() - vertices[:, 0].min()
            y_range = vertices[:, 1].max() - vertices[:, 1].min()
            z_range = vertices[:, 2].max() - vertices[:, 2].min()

            max_range = max(x_range, y_range, z_range)
            x_ratio = x_range / max_range
            y_ratio = y_range / max_range
            z_ratio = z_range / max_range

            # Spin up the main 3D figure.
            fig = go.Figure()

            # Drop in the main 3D mesh for the ring.
            fig.add_trace(
                go.Mesh3d(
                    x=vertices[:, 0],
                    y=vertices[:, 1],
                    z=vertices[:, 2],
                    i=faces[:, 0],
                    j=faces[:, 1],
                    k=faces[:, 2],
                    opacity=1,
                    color=color,
                    showscale=False,
                )
            )

            # Fake a simple shadow by adding an offset, semi-transparent mesh under it.
            shadow_offset = 0.1
            fig.add_trace(
                go.Mesh3d(
                    x=vertices[:, 0] + shadow_offset,
                    y=vertices[:, 1] + shadow_offset,
                    z=vertices[:, 2] - shadow_offset,
                    i=faces[:, 0],
                    j=faces[:, 1],
                    k=faces[:, 2],
                    opacity=0.3,
                    color="black",
                    showscale=False,
                )
            )

            # Strip out axes, grids, and titles so the ring just floats in space.
            fig.update_layout(
                scene=dict(
                    xaxis=dict(
                        showbackground=False,
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        title="",
                    ),
                    yaxis=dict(
                        showbackground=False,
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        title="",
                    ),
                    zaxis=dict(
                        showbackground=False,
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        title="",
                    ),
                    aspectratio=dict(x=x_ratio, y=y_ratio, z=z_ratio),
                    annotations=[],
                ),
                margin=dict(l=0, r=0, b=0, t=30),
                hovermode=False,
            )

            # Convert the Plotly figure into embeddable HTML.
            html_content = fig.to_html(full_html=False, include_plotlyjs="cdn")

            # Save the HTML file and wire the right thumbnail field up to its URL.
            if index == 0:
                with open(html_file_path, "w") as f:
                    f.write(html_content)
                ring.cad_file_thumbnail = html_file_url
                ring.save()
            elif index == 1:
                with open(html_file_path_2, "w") as f:
                    f.write(html_content)
                ring.cad_file_thumbnail_2 = html_file_url_2
                ring.save()
            elif index == 2:
                with open(html_file_path_3, "w") as f:
                    f.write(html_content)
                ring.cad_file_thumbnail_3 = html_file_url_3
                ring.save()

            print("saved ring to model")

        # Return a list of all three possible HTML URLs (some may not exist if there are fewer CAD files).
        urls_list = [html_file_url, html_file_url_2, html_file_url_3]
        return HttpResponse(urls_list, content_type="text/plain")